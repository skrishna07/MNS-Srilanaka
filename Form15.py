import pandas as pd
import json
import mysql.connector
from PythonLogging import setup_logging
import os
import logging
from AmazonOCR import extract_text_from_pdf
from datetime import datetime
from OpenAI import split_openai
from ReadExcelConfig import create_main_config_dictionary
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from DatabaseQueries import get_db_credentials
from DatabaseQueries import update_database_single_value
from DatabaseQueries import insert_datatable_with_table_director
import traceback
from DatabaseQueries import remove_string
from DatabaseQueries import remove_text_before_marker


def get_age(DOB):
    # Given date in the "dd/mm/yyyy" format
    try:
        given_date_string = DOB

        # Parse the given date string
        given_date = datetime.strptime(given_date_string, "%Y-%m-%d")

        # Get the current date
        current_date = datetime.now()

        # Calculate the age
        age = current_date.year - given_date.year - (
                (current_date.month, current_date.day) < (given_date.month, given_date.day))
        return age
    except Exception as e:
        logging.info(f"Error in calculating age {e}")
        return None


def form15_main(db_config, config_dict, pdf_path, output_file_path, registration_no, extraction_config):
    setup_logging()
    error_count = 0
    errors = []
    try:
        map_file_sheet_name = config_dict['config_sheet']
        if not os.path.exists(extraction_config):
            raise Exception("Main Mapping File not found")
        try:
            main_df_map = pd.read_excel(extraction_config, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Below exception occurred while reading mapping file {e}")
        df_map = main_df_map[main_df_map['Form_type'] == config_dict['form15_keyword']]
        df_map['Value'] = None
        output_dataframes_list = []
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        single_nodes = single_df['Node'].unique()
        open_ai_dict = {field_name: '' for field_name in single_nodes}
        for index, row in group_df.iterrows():
            node_values = str(row['Node']).split(',')
            sub_dict = {field_name: '' for field_name in node_values}
            main_node = row['main_dict_node']
            sub_list = {main_node: [sub_dict]}
            open_ai_dict.update(sub_list)
        pdf_text = extract_text_from_pdf(pdf_path)
        form15_prompt = config_dict['form15_prompt'] + '\n' + str(open_ai_dict) + '\n' + config_dict['form15_note_prompt']
        output = split_openai(pdf_text, form15_prompt)
        output = remove_text_before_marker(output, "```json")
        output = remove_string(output, "```")
        logging.info(output)
        nic_list = []
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        for index, row in df_map.iterrows():
            field_name = str(row.iloc[0]).strip()
            dict_node = str(row.iloc[2]).strip()
            main_group_node = str(row.iloc[6]).strip()
            type = str(row.iloc[1]).strip()
            if type.lower() == 'single':
                value = output.get(dict_node)
            elif type.lower() == 'group':
                value = output.get(main_group_node)
            else:
                value = None
            df_map.at[index, 'Value'] = value
            if field_name == 'directors' or field_name == 'secretaries':
                for sub_value in value:
                    try:
                        nic = sub_value['NIC']
                    except Exception as e:
                        nic = None
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                    nic_list.append(nic)
        logging.info(df_map)
        registration_no_column_name = config_dict['registration_no_Column_name']
        nic_url = config_dict['nic_url']
        nic_dob_gender_details = get_gender_dob(nic_url, nic_list)
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        sql_tables_list = single_df[single_df.columns[3]].unique()
        for table_name in sql_tables_list:
            table_df = single_df[single_df[single_df.columns[3]] == table_name]
            columns_list = table_df[table_df.columns[4]].unique()
            for column_name in columns_list:
                logging.info(column_name)
                # filter table df with only column value
                column_df = table_df[table_df[table_df.columns[4]] == column_name]
                logging.info(column_df)
                # create json dict with keys of field name and values for the same column name entries
                json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                # Convert the dictionary to a JSON string
                json_string = json.dumps(json_dict)
                logging.info(json_string)
                try:
                    update_database_single_value(db_config, table_name, registration_no_column_name,
                                                 registration_no,
                                                 column_name, json_string)
                except Exception as e:
                    logging.error(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                                  f"with data {json_string}")
                    error_count += 1
                    tb = traceback.extract_tb(e.__traceback__)
                    for frame in tb:
                        if frame.filename == __file__:
                            errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        for index, row in group_df.iterrows():
            try:
                field_name = str(row.iloc[0]).strip()
                nodes = str(row.iloc[2]).strip()
                sql_table_name = str(row.iloc[3]).strip()
                column_names = str(row.iloc[4]).strip()
                main_group_node = str(row.iloc[6]).strip()
                value_list = row['Value']
                if len(value_list) == 0:
                    logging.info(f"No value for {field_name} so going to next field")
                    continue
                table_df = pd.DataFrame(value_list)
                logging.info(table_df)
                column_names_list = column_names.split(',')
                column_names_list = [x.strip() for x in column_names_list]
                if sql_table_name == 'authorized_signatories':
                    table_df['age'] = None
                    table_df['nationality'] = None
                    table_df['date_of_birth'] = None
                    table_df['gender'] = None
                    column_names_list.append('age')
                    column_names_list.append('nationality')
                    column_names_list.append('date_of_birth')
                    column_names_list.append('gender')
                    for index_dob, row_dob in table_df.iterrows():
                        try:
                            nic = str(row_dob['NIC']).strip()
                            details_dict = nic_dob_gender_details.get(nic)
                            date_of_birth = details_dict['Date of Birth']
                            gender = details_dict['Gender']
                            nationality = details_dict['Nationality']
                            age = None
                            if date_of_birth is not None and gender is not None:
                                age = get_age(date_of_birth)
                            table_df.at[index_dob, 'age'] = age
                            table_df.at[index_dob, 'nationality'] = nationality
                            table_df.at[index_dob, 'date_of_birth'] = date_of_birth
                            table_df.at[index_dob, 'gender'] = gender
                        except Exception as e:
                            logging.error(f"Exception {e} occurred while getting age")
                            error_count += 1
                            tb = traceback.extract_tb(e.__traceback__)
                            for frame in tb:
                                if frame.filename == __file__:
                                    errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                if sql_table_name == 'current_shareholdings':
                    table_df['percentage_holding'] = None
                    column_names_list.append('percentage_holding')
                    try:
                        total_equity_shares = single_df[single_df['Field_Name'] == 'total_equity_shares']['Value'].values[0]
                        total_equity_shares = str(total_equity_shares)
                        total_equity_shares = str(total_equity_shares).replace(',','')
                        total_equity_shares = int(total_equity_shares)
                        for index_share, row_share in table_df.iterrows():
                            try:
                                no_of_shares = str(row_share['no_of_shares'])
                                no_of_shares = no_of_shares.replace(',', '')
                                no_of_shares = int(no_of_shares)
                                percentage_holding = (no_of_shares / total_equity_shares)*100
                            except Exception as e:
                                logging.error(f"Error fetching percentage holding {e}")
                                percentage_holding = None
                                error_count += 1
                                tb = traceback.extract_tb(e.__traceback__)
                                for frame in tb:
                                    if frame.filename == __file__:
                                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                            table_df.at[index_share, 'percentage_holding'] = percentage_holding
                    except Exception as e:
                        logging.error(f"Error in fetching percentage holding {e}")
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                table_df[registration_no_column_name] = registration_no
                column_names_list.append(registration_no_column_name)
                column_names_list = [x.strip() for x in column_names_list]
                table_df.columns = column_names_list
                for _, df_row in table_df.iterrows():
                    try:
                        insert_datatable_with_table_director(config_dict, db_config, sql_table_name, column_names_list,
                                                             df_row)
                    except Exception as e:
                        logging.info(
                            f'Exception {e} occurred while inserting below table row in table {sql_table_name}- \n',
                            df_row)
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
            except Exception as e:
                logging.error(f"Exception occurred while inserting for group values {e}")
                error_count += 1
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        output_dataframes_list.append(single_df)
        output_dataframes_list.append(group_df)
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for dataframe in output_dataframes_list:
                # logging.info(dataframe)
                dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(dataframe.index) + 2
        output_dataframes_list.clear()
    except Exception as e:
        logging.error(f"Error while extracting data for Form 15 {e}")
        error_count += 1
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info(f"Successfully extracted for Form 15")
            return True
        else:
            raise Exception(errors)


def get_gender_dob(url, nic_list):
    try:
        setup_logging()
        for _ in range(0, 3):
            try:
                options = Options()
                options.add_argument('--headless')
                driver = webdriver.Chrome(options=options)
                driver.get(url)
                time.sleep(5)
                master_dict = {}
                for nic in nic_list:
                    date_of_birth = None
                    gender = None
                    formatted_date = None
                    nationality = None
                    nic_input_xpath = '//input[@type="text" and @class="login-input" ]'
                    nic_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, nic_input_xpath))
                    )
                    if nic_element:
                        nic_element.clear()
                        nic_element.send_keys(nic)
                        time.sleep(1)
                        generate_xpath = '//input[@type="submit" and @class="login-submit"]'
                        generate_element = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, generate_xpath)))
                        generate_element.click()
                        time.sleep(5)
                        date_of_birth_xpath = '//th[@id = "dob"]'
                        try:
                            date_of_birth_element = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, date_of_birth_xpath)))
                            date_of_birth = date_of_birth_element.text
                            gender_xpath = '//th[@id = "gender"]'
                            gender_element = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.XPATH, gender_xpath)))
                            gender = gender_element.text
                            logging.info(f"{date_of_birth} {gender}")
                            nationality = 'Sri Lankan'
                            try:
                                input_format = "%b, %d %Y"
                                # Parse the date string to a datetime object
                                date_obj = datetime.strptime(date_of_birth, input_format)
                                # Define the output format
                                output_format = "%Y-%m-%d"
                                # Convert the datetime object to the desired string format
                                formatted_date = date_obj.strftime(output_format)
                            except Exception as e:
                                logging.error(f"Exception occurred in converting date to yyyy-mm-dd {e}")
                                formatted_date = date_of_birth
                        except Exception as e:
                            try:
                                WebDriverWait(driver, 10).until(EC.alert_is_present())
                                # Switch to the alert
                                alert = driver.switch_to.alert
                                # Accept the alert (click the OK button)
                                alert.accept()
                                logging.info("Invalid NIC Number")
                            except Exception as e:
                                logging.error(f"Exception occurred here {e}")
                        master_dict[nic] = {
                            'Gender': gender,
                            'Date of Birth': formatted_date,
                            'Nationality': nationality
                        }
                driver.close()
                return master_dict
            except Exception as e:
                logging.error(f"Exception {e} occurred")
                try:
                    driver.close()
                except:
                    pass
    except Exception as e:
        logging.error(f"Error in getting date of birth and gender {e}")
        raise Exception(e)

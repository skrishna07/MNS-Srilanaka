import pandas as pd
import json
import mysql.connector
from PythonLogging import setup_logging
import os
import logging
from AmazonOCR import extract_text_from_pdf
from OpenAI import split_openai
from ReadExcelConfig import create_main_config_dictionary
from DatabaseQueries import get_db_credentials
import traceback
from Form15 import get_gender_dob
from datetime import datetime
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


def insert_datatable_with_table_form20(config_dict, db_config, sql_table_name, column_names_list, df_row , field_name):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    db_connection.autocommit = True

    combined = list(zip(column_names_list, df_row))
    # Create a dictionary from the list of tuples
    result_dict = dict(combined)
    # logging.info(result_dict)
    registration_column_name = config_dict['registration_no_Column_name']
    registration_no = result_dict[registration_column_name]
    name_column_name = config_dict['name_column_name_in_db_directors']
    name = result_dict[name_column_name]
    name = name.lower()
    nic_column_name = config_dict['nic_Column_name_directors']
    designation_column_name = config_dict['designation_column_name']
    designation = result_dict[designation_column_name]
    address_column_name = config_dict['address_column_name']
    address = result_dict[address_column_name]
    email_column_name = config_dict['email_column_name']
    event_date_column_name = config_dict['event_date_column_name']
    event_date = result_dict[event_date_column_name]
    event_column_name = config_dict['event_column_name']
    event = result_dict[event_column_name]
    select_query = (
        f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}"')
    logging.info(select_query)
    db_cursor.execute(select_query)
    result = db_cursor.fetchall()
    logging.info(len(result))
    name_found = False
    for directors in result:
        db_name = directors[2]
        db_name = str(db_name).lower()
        intersection = set(db_name) & set(name)
        union = set(db_name) | set(name)
        similarity = len(intersection) / len(union)
        # Convert to percentage
        percentage_match = similarity * 100
        if percentage_match > 90:
            logging.info(f"Form 20 name {name} matched with db name {db_name}")
            name_found = True
            result_dict[name_column_name] = db_name
            event_check_query = f'select event from {sql_table_name} where {registration_column_name} = "{registration_no}" and {name_column_name} = "{db_name}"'
            logging.info(event_check_query)
            db_cursor.execute(event_check_query)
            event_result = db_cursor.fetchone()[0]
            if event_result is None or event_result == '':
                logging.info(f"Updating as no event was updated before for director {db_name}")
                result_dict.pop(registration_column_name)
                result_dict.pop(name_column_name)
                result_dict.pop(designation_column_name)
                result_dict.pop(address_column_name)
                column_names_list = list(column_names_list)
                column_names_list.remove(registration_column_name)
                column_names_list.remove(name_column_name)
                column_names_list.remove(designation_column_name)
                column_names_list.remove(address_column_name)
                if field_name == 'appointment_directors':
                    result_dict.pop(nic_column_name)
                    result_dict.pop(email_column_name)
                    column_names_list.remove(nic_column_name)
                    column_names_list.remove(email_column_name)
                update_query = f'''UPDATE {sql_table_name}
                                                                                           SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                                                                           WHERE {registration_column_name} = "{registration_no}" AND {name_column_name} = "{db_name}"'''
                logging.info(update_query)
                db_cursor.execute(update_query)
                logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
                break
            select_query_director = (
        f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" and {name_column_name} = "{db_name}" and LOWER({event_column_name}) = "{str(event).lower()}" and {event_date_column_name} = "{event_date}"')
            logging.info(select_query_director)
            db_cursor.execute(select_query_director)
            director_result = db_cursor.fetchall()
            if len(director_result) == 0:
                logging.info(f"Director is found but with different event.So inserting this new event")
                insert_query = f'''
                                INSERT INTO {sql_table_name}
                                SET {', '.join([f"{col} = %s" for col in column_names_list])};
                                '''
                logging.info(insert_query)
                logging.info(tuple(df_row.values))
                db_cursor.execute(insert_query, tuple(df_row.values))
            else:
                logging.info(f"Director with same event and date found.So updating")
                result_dict.pop(registration_column_name)
                result_dict.pop(name_column_name)
                result_dict.pop(designation_column_name)
                result_dict.pop(address_column_name)
                column_names_list = list(column_names_list)
                column_names_list.remove(registration_column_name)
                column_names_list.remove(name_column_name)
                column_names_list.remove(designation_column_name)
                column_names_list.remove(address_column_name)
                if field_name == 'appointment_directors':
                    result_dict.pop(nic_column_name)
                    result_dict.pop(email_column_name)
                    column_names_list.remove(nic_column_name)
                    column_names_list.remove(email_column_name)
                update_query = f'''UPDATE {sql_table_name}
                                                                            SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                                                            WHERE {registration_column_name} = "{registration_no}" AND {name_column_name} = "{db_name}"'''
                logging.info(update_query)
                db_cursor.execute(update_query)
                logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
            break
    if not name_found:  # If no matching record found
        # Insert the record
        logging.info(f"Director details not found in database so inserting along with event and event_date")
        insert_query = f'''
                INSERT INTO {sql_table_name}
                SET {', '.join([f"{col} = %s" for col in column_names_list])};
                '''
        logging.info(insert_query)
        logging.info(tuple(df_row.values))
        db_cursor.execute(insert_query, tuple(df_row.values))
        # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
    db_cursor.close()
    db_connection.close()


def form20_main(db_config, config_dict, pdf_path, output_file_path, registration_no, extraction_config):
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
        df_map = main_df_map[main_df_map['Form_type'] == config_dict['form20_keyword']]
        df_map['Value'] = None
        open_ai_dict = {}
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        for index, row in group_df.iterrows():
            node_values = str(row['Node']).split(',')
            sub_dict = {field_name: '' for field_name in node_values}
            main_node = row['main_dict_node']
            sub_list = {main_node: [sub_dict]}
            open_ai_dict.update(sub_list)
        pdf_text = extract_text_from_pdf(pdf_path)
        form20_prompt = config_dict['form20_prompt'] + '\n' + str(open_ai_dict)
        output = split_openai(pdf_text, form20_prompt)
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
            if type.lower() == 'group':
                value = output.get(main_group_node)
            else:
                value = None
            df_map.at[index, 'Value'] = value
            if field_name == 'appointment_directors':
                for sub_value in value:
                    try:
                        nic = sub_value['nic']
                    except Exception as e:
                        nic = None
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                    nic_list.append(nic)
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        registration_no_column_name = config_dict['registration_no_Column_name']
        nic_url = config_dict['nic_url']
        nic_dob_gender_details = get_gender_dob(nic_url, nic_list)
        for index, row in group_df.iterrows():
            try:
                field_name = str(row.iloc[0]).strip()
                nodes = str(row.iloc[2]).strip()
                sql_table_name = str(row.iloc[3]).strip()
                column_names = str(row.iloc[4]).strip()
                main_group_node = str(row.iloc[6]).strip()
                value_list = row['Value']
                table_df = pd.DataFrame(value_list)
                logging.info(table_df)
                column_names_list = column_names.split(',')
                column_names_list = [x.strip() for x in column_names_list]
                if sql_table_name == 'authorized_signatories' and field_name == 'appointment_directors':
                    table_df['age'] = None
                    table_df['nationality'] = None
                    table_df['date_of_birth'] = None
                    table_df['gender'] = None
                    table_df['date_of_appointment'] = None
                    column_names_list.append('age')
                    column_names_list.append('nationality')
                    column_names_list.append('date_of_birth')
                    column_names_list.append('gender')
                    column_names_list.append('date_of_appointment')
                    for index_dob, row_dob in table_df.iterrows():
                        try:
                            nic = str(row_dob['nic']).strip()
                            date = str(row_dob['date']).strip()
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
                            table_df.at[index_dob, 'date_of_appointment'] = date
                        except Exception as e:
                            logging.error(f"Exception {e} occurred while getting age")
                            error_count += 1
                            tb = traceback.extract_tb(e.__traceback__)
                            for frame in tb:
                                if frame.filename == __file__:
                                    errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                if sql_table_name == 'authorized_signatories' and field_name == 'cessation_directors':
                    table_df['date_of_cessation'] = None
                    column_names_list.append('date_of_cessation')
                    for index_cessation, row_cessation in table_df.iterrows():
                        try:
                            cessation_date = str(row_cessation['date']).strip()
                            table_df.at[index_cessation, 'date_of_cessation'] = cessation_date
                        except Exception as e:
                            logging.error(f"Exception {e} occurred while getting age")
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
                        insert_datatable_with_table_form20(config_dict, db_config, sql_table_name, column_names_list,
                                                             df_row, field_name)
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
                logging.error(f"Error occurred while inserting for group values")
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                error_count += 1
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            group_df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
    except Exception as e:
        logging.error(f"Error occurred in extracting data for Form 20 {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info(f"Successfully extracted for Form 6")
            return True
        else:
            raise Exception(f"Multiple exceptions occurred:\n\n" + "\n".join(errors))

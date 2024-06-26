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
from DatabaseQueries import update_database_single_value
import traceback


def insert_datatable_with_table_director_form6(config_dict, db_config, sql_table_name, column_names_list, df_row, form_date):
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

    if sql_table_name == 'authorized_signatories':
        name_column_name = config_dict['name_column_name_in_db_directors']
        nic_column_name = config_dict['nic_Column_name_directors']
        name = result_dict[name_column_name]
        nic = result_dict[nic_column_name]
        select_query = (
            f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND {name_column_name}'
            f' = "{name}" and {nic_column_name} = "{nic}"')
        logging.info(select_query)
        db_cursor.execute(select_query)
        result = db_cursor.fetchall()
        logging.info(len(result))
        if len(result) == 0:  # If no matching record found
            # Insert the record
            insert_query = f'''
            INSERT INTO {sql_table_name}
            SET {', '.join([f"{col} = %s" for col in column_names_list])};
            '''
            logging.info(insert_query)
            logging.info(tuple(df_row.values))
            db_cursor.execute(insert_query, tuple(df_row.values))
            # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
        else:
            result_dict.pop(registration_column_name)
            result_dict.pop(name_column_name)
            result_dict.pop(nic_column_name)
            column_names_list = list(column_names_list)
            column_names_list.remove(registration_column_name)
            column_names_list.remove(name_column_name)
            column_names_list.remove(nic_column_name)
            update_query = f'''UPDATE {sql_table_name}
                                                        SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                                        WHERE {registration_column_name} = "{registration_no}" AND {name_column_name} = "{name}" and {nic_column_name} = "{nic}"'''
            logging.info(update_query)
            db_cursor.execute(update_query)
            logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    else:
        if sql_table_name == 'current_shareholdings':
            name_column_name = config_dict['name_column_name_in_db_shareholders']
        elif sql_table_name == 'auditor':
            name_column_name = config_dict['name_column_name_in_db_auditors']
        else:
            raise Exception("Invalid table")
        name = result_dict[name_column_name]
        select_query = (f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND {name_column_name}'
                        f' = "{name}"')
        logging.info(select_query)
        db_cursor.execute(select_query)
        result = db_cursor.fetchall()
        logging.info(len(result))
        if len(result) == 0:  # If no matching record found
            # Insert the record
            insert_query = f'''
            INSERT INTO {sql_table_name}
            SET {', '.join([f"{col} = %s" for col in column_names_list])};
            '''
            logging.info(insert_query)
            logging.info(tuple(df_row.values))
            db_cursor.execute(insert_query, tuple(df_row.values))
            # logging.info(f"Data row values are saved in table {sql_table_name} with \n {df_row}")
        else:
            result_dict.pop(registration_column_name)
            result_dict.pop(name_column_name)
            column_names_list = list(column_names_list)
            column_names_list.remove(registration_column_name)
            column_names_list.remove(name_column_name)
            date_check_select_query = (f'SELECT updated_date FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND {name_column_name}'
                        f'= "{name}"')
            logging.info(date_check_select_query)
            db_cursor.execute(date_check_select_query)
            updated_date = db_cursor.fetchone()[0]
            if updated_date == form_date:
                logging.info(f"Data already there for this director {name} for form 6 at date {form_date}.So not updating")
            else:
                update_query = f'''UPDATE {sql_table_name} SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                WHERE {registration_column_name} = "{registration_no}" AND {name_column_name} = "{name}"'''
                logging.info(update_query)
                db_cursor.execute(update_query)
                logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    db_cursor.close()
    db_connection.close()


def get_previous_total_equity_shares(db_config, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = f"select total_equity_shares from shareholdings_summary where registration_no = '{registration_no}'"
        logging.info(query)
        cursor.execute(query)
        total_equity_shares = cursor.fetchone()[0]
        logging.info(f"Total equity shares from Form 15 {total_equity_shares}")
        cursor.close()
        connection.close()
        try:
            total_equity_shares = str(total_equity_shares).replace(',','')
            total_equity_shares = int(total_equity_shares)
        except:
            pass
        return total_equity_shares
    except Exception as e:
        logging.error(f"Error in fetching equity shares {e}")
        raise Exception(e)


def get_current_shareholdings_details(db_config, registration_no, name):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = f"select full_name,no_of_shares from current_shareholdings where registration_no = '{registration_no}'"
        logging.info(query)
        cursor.execute(query)
        full_form15_name_list = cursor.fetchall()
        cursor.close()
        connection.close()
        for details in full_form15_name_list:
            previous_name = details[0]
            no_of_shares = details[1]
            previous_name = previous_name.lower()
            name = name.lower()
            # Find intersection and union
            intersection = set(previous_name) & set(name)
            union = set(previous_name) | set(name)
            similarity = len(intersection) / len(union)
            # Convert to percentage
            percentage_match = similarity * 100
            if percentage_match > 90:
                logging.info(f"Matched {previous_name} with given form 6 name {name} with percentage match {percentage_match}")
                return previous_name, no_of_shares
        return None, None
    except Exception as e:
        logging.error(f"Error occurred in fetching current shareholdings from form 15 {e}")
        raise Exception(e)


def form6_main(db_config, config_dict, pdf_path, output_file_path, registration_no , form_date, extraction_config):
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
        df_map = main_df_map[main_df_map['Form_type'] == config_dict['form6_keyword']]
        df_map['Value'] = None
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        output_dataframes_list = []
        registration_no_column_name = config_dict['registration_no_Column_name']
        single_nodes = single_df['Node'].unique()
        open_ai_dict = {field_name: '' for field_name in single_nodes if field_name != 'updated_date'}
        for index, row in group_df.iterrows():
            node_values = str(row['Node']).split(',')
            sub_dict = {field_name: '' for field_name in node_values}
            main_node = row['main_dict_node']
            sub_list = {main_node: [sub_dict]}
            open_ai_dict.update(sub_list)
        pdf_text = extract_text_from_pdf(pdf_path)
        form15_prompt = config_dict['form6_prompt'] + '\n' + str(open_ai_dict)
        output = split_openai(pdf_text, form15_prompt)
        logging.info(output)
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        for index, row in df_map.iterrows():
            field_name = str(row.iloc[0]).strip()
            dict_node = str(row.iloc[2]).strip()
            main_group_node = str(row.iloc[6]).strip()
            type = str(row.iloc[1]).strip()
            if field_name == 'updated_date':
                value = form_date
                df_map.at[index, 'Value'] = value
                continue
            if type.lower() == 'single':
                value = output.get(dict_node)
            elif type.lower() == 'group':
                value = output.get(main_group_node)
            else:
                value = None
            if field_name == 'total_equity_shares':
                updated_equity_shares_value = get_previous_total_equity_shares(db_config, registration_no)
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                date_check_query = (
                    f'SELECT updated_date FROM shareholdings_summary WHERE {registration_no_column_name} = "{registration_no}"')
                logging.info(date_check_query)
                cursor.execute(date_check_query)
                equity_shares_date = cursor.fetchone()[0]
                cursor.close()
                connection.close()
                if equity_shares_date == form_date:
                    value = updated_equity_shares_value
                    logging.info(
                        f"Already form 6 ran for date {form_date} for total_equity_shares.So keeping the value {value} same")
                    df_map.at[index, 'Value'] = value
                    continue
                else:
                    try:
                        value = str(value).replace(',', '')
                        value = int(value)
                    except Exception as e:
                        error_count += 1
                        tb = traceback.extract_tb(e.__traceback__)
                        for frame in tb:
                            if frame.filename == __file__:
                                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                    logging.info(f"Adding old value {value} to new {updated_equity_shares_value} for equity shares")
                    value = value + updated_equity_shares_value
            df_map.at[index, 'Value'] = value
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        registration_no_column_name = config_dict['registration_no_Column_name']
        updated_date_column_name = config_dict['updated_date_column_name']
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
                table_df = pd.DataFrame(value_list)
                logging.info(table_df)
                column_names_list = column_names.split(',')
                column_names_list = [x.strip() for x in column_names_list]
                if sql_table_name == 'current_shareholdings':
                    table_df['percentage_holding'] = None
                    column_names_list.append('percentage_holding')
                    total_equity_shares = single_df[single_df['Field_Name'] == 'total_equity_shares']['Value'].values[0]
                    total_equity_shares = str(total_equity_shares).replace(',', '')
                    total_equity_shares = int(total_equity_shares)
                    for index_share, row_share in table_df.iterrows():
                        no_of_shares = row_share['no_of_shares']
                        name = row_share['name_of_shareholder']
                        form15_name, form15_no_of_shares = get_current_shareholdings_details(db_config, registration_no, name)
                        if form15_name is not None and form15_no_of_shares is not None:
                            logging.info(f"{name} is already present in form 15 so updating")
                            table_df.at[index_share, 'name_of_shareholder'] = form15_name
                            try:
                                no_of_shares = str(no_of_shares).replace(',','')
                                no_of_shares = int(no_of_shares)
                                form15_no_of_shares = str(form15_no_of_shares).replace(',','')
                                form15_no_of_shares = int(form15_no_of_shares)
                                updated_no_of_shares = no_of_shares + form15_no_of_shares
                            except Exception as e:
                                updated_no_of_shares = no_of_shares
                                error_count += 1
                                tb = traceback.extract_tb(e.__traceback__)
                                for frame in tb:
                                    if frame.filename == __file__:
                                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                            percentage_holding = (updated_no_of_shares / total_equity_shares) * 100
                            table_df.at[index_share, 'no_of_shares'] = updated_no_of_shares
                            table_df.at[index_share, 'percentage_holding'] = percentage_holding
                        else:
                            logging.info(f"These details are not there in form 15 so inserting from form 6 for name - {name}")
                            try:
                                no_of_shares = row_share['no_of_shares'].replace(',', '')
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
                table_df[registration_no_column_name] = registration_no
                table_df[updated_date_column_name] = form_date
                column_names_list.append(registration_no_column_name)
                column_names_list.append(updated_date_column_name)
                column_names_list = [x.strip() for x in column_names_list]
                table_df.columns = column_names_list
                for _, df_row in table_df.iterrows():
                    try:
                        insert_datatable_with_table_director_form6(config_dict, db_config, sql_table_name, column_names_list,
                                                             df_row, form_date)
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
                logging.error(f"Exception occurred while inserting group values {e}")
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
                error_count += 1
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
        logging.error(f"Exception occurred while extracting data for form 6 {e}")
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

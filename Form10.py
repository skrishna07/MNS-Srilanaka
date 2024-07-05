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
import re


def insert_datatable_with_table_form10(config_dict, db_config, sql_table_name, column_names_list, df_row):
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
    holder_name_column_name = config_dict['holder_name_column_name_charges']
    charge_id_column_name = config_dict['charge_id_column_name']
    holder_name = result_dict[holder_name_column_name]
    charge_id = result_dict[charge_id_column_name]
    select_query = (
        f'SELECT * FROM {sql_table_name} WHERE {registration_column_name} = "{registration_no}" AND {holder_name_column_name}'
        f' = "{holder_name}" and {charge_id_column_name} = "{charge_id}"')
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
        result_dict.pop(holder_name_column_name)
        result_dict.pop(charge_id_column_name)
        column_names_list = list(column_names_list)
        column_names_list.remove(registration_column_name)
        column_names_list.remove(holder_name_column_name)
        column_names_list.remove(charge_id_column_name)
        update_query = f'''UPDATE {sql_table_name}
                                                            SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                                                            WHERE {registration_column_name} = "{registration_no}" AND {holder_name_column_name} = "{holder_name}" and {charge_id_column_name} = "{charge_id}"'''
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    db_cursor.close()
    db_connection.close()


def form10_main(db_config, config_dict, pdf_path, output_file_path, registration_no, extraction_config):
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
        df_map = main_df_map[main_df_map['Form_type'] == config_dict['form10_keyword']].copy()
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
        form10_prompt = config_dict['form10_prompt'] + '\n' + str(open_ai_dict)
        output = split_openai(pdf_text, form10_prompt)
        try:
            output = re.sub(r'(?<=: ")(\d+(,\d+)*)(?=")', lambda x: x.group(1).replace(",", ""), output)
        except:
            pass
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
            if type.lower() == 'group':
                value = output.get(main_group_node)
            else:
                value = None
            df_map.at[index, 'Value'] = value
        group_df = df_map[df_map[df_map.columns[1]] == config_dict['group_keyword']]
        registration_no_column_name = config_dict['registration_no_Column_name']
        type_column_name = config_dict['type_column_name']
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
                table_df[registration_no_column_name] = registration_no
                table_df[type_column_name] = config_dict['default_type_value_charges']
                column_names_list.append(registration_no_column_name)
                column_names_list.append(type_column_name)
                column_names_list = [x.strip() for x in column_names_list]
                table_df.columns = column_names_list
                for _, df_row in table_df.iterrows():
                    try:
                        insert_datatable_with_table_form10(config_dict, db_config, sql_table_name, column_names_list,
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
        logging.error(f"Exception occurred while extracting data for Form 10 {e}")
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

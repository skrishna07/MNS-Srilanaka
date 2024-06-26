import pandas as pd
import json
from PythonLogging import setup_logging
import os
import logging
from AmazonOCR import extract_text_from_pdf
from OpenAI import split_openai
from ReadExcelConfig import create_main_config_dictionary
from DatabaseQueries import get_db_credentials
from DatabaseQueries import update_database_single_value
import traceback


def form40and3and13_main(db_config, config_dict, pdf_path, output_file_path, registration_no,file_name, extraction_config):
    setup_logging()
    error_count = 0
    errors = []
    try:
        if 'form 40' in file_name.lower():
            form_keyword = config_dict['form40_keyword']
        elif 'form 3' in file_name.lower():
            form_keyword = config_dict['form3_keyword']
        elif 'form 13' in file_name.lower():
            form_keyword = config_dict['form13_keyword']
        else:
            raise Exception("Invalid Form")
        map_file_sheet_name = config_dict['config_sheet']
        if not os.path.exists(extraction_config):
            raise Exception("Main Mapping File not found")
        try:
            main_df_map = pd.read_excel(extraction_config, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Below exception occurred while reading mapping file {e}")
        df_map = main_df_map[main_df_map['Form_type'] == form_keyword].copy()
        df_map['Value'] = None
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        single_nodes = single_df['Node'].unique()
        open_ai_dict = {field_name: '' for field_name in single_nodes}
        pdf_text = extract_text_from_pdf(pdf_path)
        form10_prompt = config_dict['common_prompt'] + '\n' + str(open_ai_dict)
        output = split_openai(pdf_text, form10_prompt)
        logging.info(output)
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        for index, row in df_map.iterrows():
            dict_node = str(row.iloc[2]).strip()
            type = str(row.iloc[1]).strip()
            if type.lower() == 'single':
                value = output.get(dict_node)
            else:
                value = None
            df_map.at[index, 'Value'] = value
        single_df = df_map[df_map[df_map.columns[1]] == config_dict['single_keyword']]
        registration_no_column_name = config_dict['registration_no_Column_name']
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
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            single_df.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
            row_index += len(single_df.index) + 2
    except Exception as e:
        logging.error(f"Error in extracting data from Form 40 {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info(f"Successfully extracted for Form 40")
            return True
        else:
            raise Exception(f"Multiple exceptions occurred:\n\n" + "\n".join(errors))

import json
import PyPDF2
from AmazonOCR import extract_text_from_pdf as amazon_ocr
from OpenAI import split_openai
from PythonLogging import setup_logging
import logging
import traceback
import os
import pandas as pd
import re
from DatabaseQueries import update_database_single_value_financial


def find_header_and_next_pages(pdf_path, header_fields, negative_fields ,fields):
    # Open the PDF file
    pdf_file = open(pdf_path, 'rb')
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    num_pages = len(pdf_reader.pages)
    # Search for the header in the PDF
    for page_num in range(0, num_pages, 2):
        # Read the current page and the next page
        page1 = pdf_reader.pages[page_num]
        text1 = page1.extract_text()

        text2 = ""
        if page_num + 1 < num_pages:
            page2 = pdf_reader.pages[page_num + 1]
            text2 = page2.extract_text()

        # Combine text from both pages
        combined_text = (text1 or "") + "\n" + (text2 or "")
        combined_text = combined_text.replace(',', '')
        numbers = re.findall(r'\b\d{5,}\b', combined_text)
        # print(combined_text.lower())
        # print('\n')
        # print('--------------------------------------------')
        # Check if all fields are in the combined text
        if combined_text and any(field in combined_text.lower() for field in header_fields) and any(field in combined_text.lower() for field in fields) and not any(neg_field in combined_text.lower() for neg_field in negative_fields) and page_num > 20 and len(numbers) >= 10:
            next_page = page_num + 2 if page_num + 2 < num_pages else None
            return page_num, next_page
    return None, None


def split_pdf(file_path, start_page, end_page, output_path):
    setup_logging()
    try:
        pdf_reader = PyPDF2.PdfReader(open(file_path, 'rb'))
        pdf_writer = PyPDF2.PdfWriter()

        for page_num in range(start_page - 1, end_page):
            pdf_writer.add_page(pdf_reader.pages[page_num])

        with open(output_path, 'wb') as output_pdf:
            pdf_writer.write(output_pdf)
    except Exception as e:
        logging.error(f"Error in splitting pdf {e}")
        raise Exception(e)


def profit_and_loss_main(db_config, config_dict, pdf_path, registration_no, temp_pdf_path, output_file_path):
    setup_logging()
    error_count = 0
    errors = []
    try:
        header_fields = str(config_dict['profit_and_loss_headers']).split(',')
        fields = str(config_dict['profit_and_loss_fields']).split(',')
        negative_fields = str(config_dict['financial_negative_headers']).split(',')
        starting_page, ending_page = find_header_and_next_pages(pdf_path, header_fields, negative_fields, fields)
        if starting_page is not None:
            logging.info(f"Taking from page {starting_page + 1} to {ending_page + 1}")
        split_pdf(pdf_path, starting_page+1, ending_page+1, temp_pdf_path)
        extracted_text = amazon_ocr(temp_pdf_path)
        extracted_text = extracted_text.replace(',', '')
        logging.info(extracted_text)
        config_file_path = config_dict['financial_config']
        map_file_sheet_name = config_dict['config_sheet']
        if not os.path.exists(config_file_path):
            raise Exception("Main Mapping File not found")
        try:
            df_map = pd.read_excel(config_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Below exception occurred while reading mapping file {e}")
        df_map['Value'] = None
        registration_no_column_name = config_dict['registration_no_Column_name']
        financial_df = df_map[(df_map['Type_of_financial'] == config_dict['profit_and_loss_keyword']) | (df_map['Type_of_financial'] == config_dict['common_keyword'])]
        straight_df = financial_df[(financial_df[financial_df.columns[1]] == config_dict['financial_straight_keyword']) &
                            (financial_df['Node'].notna()) &
                            (financial_df['Node'] != '') &
                            (financial_df['Node'] != 'null')]
        straight_field_nodes = straight_df['Node'].unique()
        exclude_fields = ['year', 'financial_year', 'nature', 'filing_type', 'filing_standard']
        master_dict = {"Group": [{'YYYY': ""}], "Company": [{'YYYY': ""}]}
        open_ai_dict = {field_name: '' for field_name in straight_field_nodes if field_name not in exclude_fields}
        master_dict["Group"][0]["YYYY"] = str(open_ai_dict)
        master_dict["Company"][0]["YYYY"] = str(open_ai_dict)
        prompt = config_dict['profit_and_loss_prompt'] + '\n' + str(master_dict) + '\n' + '\n' + str(config_dict['financial_example_prompt'])
        output = split_openai(extracted_text, prompt)
        try:
            output = re.sub(r'(?<=: ")(\d+(,\d+)*)(?=")', lambda x: x.group(1).replace(",", ""), output)
        except:
            pass
        logging.info(output)
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        try:
            if len(output["Group"]) != 0:
                group_output = output["Group"][0]
            else:
                group_output = {}
        except:
            group_output = {}
        try:
            if len(output["Company"]) != 0:
                company_output = output["Company"][0]
            else:
                company_output = {}
        except:
            company_output = {}
        main_group_df = financial_df.copy()
        main_company_df = financial_df.copy()
        df_list = []
        for key, value in group_output.items():
            group_year_df = main_group_df.copy()
            logging.info(key)
            nature = 'Consolidated'
            group_year_df.loc[group_year_df['Field_Name'] == 'nature', 'Value'] = nature
            group_year_df.loc[group_year_df['Field_Name'] == 'year', 'Value'] = key
            group_year_df.loc[group_year_df['Field_Name'] == 'filing_type', 'Value'] = config_dict['filing_standard']
            for financial_key, financial_value in value.items():
                group_year_df.loc[group_year_df['Node'].str.strip() == str(financial_key).strip(), 'Value'] = financial_value
            formula_df = group_year_df[group_year_df[group_year_df.columns[1]] == config_dict['Formula_Keyword']]
            for _, row in formula_df.iterrows():
                group_formula = row['Node']
                group_formula_field_name = row['Field_Name']
                for field_name in group_year_df['Field_Name']:
                    field_name = str(field_name)
                    pattern = r'\b' + re.escape(field_name) + r'\b'
                    # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]))
                    replacement_value = str(
                        group_year_df[group_year_df['Field_Name'] == field_name]['Value'].values[0])
                    replacement_value = str(replacement_value) if replacement_value != '' else '0'
                    group_formula = re.sub(pattern, replacement_value, group_formula)
                logging.info(group_formula_field_name + ":" + group_formula)
                try:
                    # Calculate the value using the provided formula and insert it
                    if 'None' in group_formula:
                        group_formula = group_formula.replace('None', '0')
                    group_year_df.at[
                        group_year_df[group_year_df['Field_Name'] == group_formula_field_name].index[
                            0], 'Value'] = round(eval(group_formula), 2)
                except (NameError, SyntaxError):
                    # Handle the case where the formula is invalid or contains a missing field name
                    logging.info(f"Invalid formula for {group_formula_field_name}: {group_formula}")
            df_list.append(group_year_df)
        for key, value in company_output.items():
            company_year_df = main_company_df.copy()
            logging.info(key)
            nature = 'Standalone'
            company_year_df.loc[company_year_df['Field_Name'] == 'nature', 'Value'] = nature
            company_year_df.loc[company_year_df['Field_Name'] == 'year', 'Value'] = key
            company_year_df.loc[company_year_df['Field_Name'] == 'filing_type', 'Value'] = config_dict['filing_standard']
            for financial_key,financial_value in value.items():
                company_year_df.loc[company_year_df['Node'].str.strip() == str(financial_key).strip(), 'Value'] = financial_value
            company_formula_df = company_year_df[company_year_df[company_year_df.columns[1]] == config_dict['Formula_Keyword']]
            for _, row in company_formula_df.iterrows():
                company_formula = row['Node']
                company_formula_field_name = row['Field_Name']
                for field_name in company_year_df['Field_Name']:
                    field_name = str(field_name)
                    pattern = r'\b' + re.escape(field_name) + r'\b'
                    # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]))
                    replacement_value = str(
                        company_year_df[company_year_df['Field_Name'] == field_name]['Value'].values[0])
                    replacement_value = str(replacement_value) if replacement_value != '' else '0'
                    company_formula = re.sub(pattern, replacement_value, company_formula)
                logging.info(company_formula_field_name + ":" + company_formula)
                try:
                    # Calculate the value using the provided formula and insert it
                    if 'None' in company_formula:
                        company_formula = company_formula.replace('None', '0')
                    company_year_df.at[
                        company_year_df[company_year_df['Field_Name'] == company_formula_field_name].index[
                            0], 'Value'] = round(eval(company_formula), 2)
                except (NameError, SyntaxError):
                    # Handle the case where the formula is invalid or contains a missing field name
                    logging.info(f"Invalid formula for {company_formula_field_name}: {company_formula}")
            df_list.append(company_year_df)
        for df in df_list:
            sql_tables_list = df[df.columns[3]].unique()
            logging.info(sql_tables_list)
            year_value = df[df['Field_Name'] == 'year']['Value'].values[0]
            nature_value = df[df['Field_Name'] == 'nature']['Value'].values[0]
            logging.info(year_value)
            for table_name in sql_tables_list:
                table_df = df[df[df.columns[3]] == table_name]
                columns_list = table_df[table_df.columns[4]].unique()
                logging.info(columns_list)
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
                        update_database_single_value_financial(db_config, table_name, registration_no_column_name, registration_no, column_name, json_string, year_value, nature_value)
                    except Exception as e:
                        logging.info(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                                     f"with data {json_string}")
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for dataframe in df_list:
                # logging.info(dataframe)
                dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(dataframe.index) + 2
        df_list.clear()
    except Exception as e:
        logging.error(f"Error in extracting financial data for reg no - {registration_no}")
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

from Form6 import form6_main
from Form10 import form10_main
from Form15 import form15_main
from Form20 import form20_main
from Form40and3and13 import form40and3and13_main
from PythonLogging import setup_logging
import logging
import traceback
from DatabaseQueries import get_documents_to_extract
from DatabaseQueries import extraction_pending_files
from DatabaseQueries import update_extraction_status
from JSONLoaderGeneration import json_loader
from ReadExcelConfig import create_main_config_dictionary
from OrderJson import order_json
from FinalEmailTable import final_table
from DatabaseQueries import form_check
from DatabaseQueries import update_form_extraction_status
from AddressSplit import split_address
from DatabaseQueries import update_extraction_needed_status_to_n
import time
from Form6_SameDateCheck import form6_same_date_check
from S_Finance import finance_main
from ProfitandLoss import profit_and_loss_main
import os
from DatabaseQueries import get_financial_status
from DatabaseQueries import update_finance_status
from DatabaseQueries import update_pnl_status
from FinalEmailTable import form13_table
from Form6 import update_form15_percentage_holding
from Holding_Entities import get_holding_entities
from FinalEmailTable import financials_table
from DatabaseQueries import get_split_status
from Split_Scanned_Pdf import split_pdf_based_on_headers_and_fields
from DatabaseQueries import update_split_status_and_split_pdf_path
from New_tags_table import new_tags_table


def data_extraction_and_insertion(db_config, registration_no, config_dict):
    setup_logging()
    error_count = 0
    errors = []
    try:
        form_extraction_needed = update_form_extraction_status(db_config, registration_no, config_dict)
        if form_extraction_needed:
            logging.info(f"Successfully updated form extraction needed for forms")
        time.sleep(2)
        documents_to_extract = get_documents_to_extract(db_config, registration_no)
        document_name = None
        document_download_path = None
        for document in documents_to_extract:
            try:
                document_id = document[0]
                document_name = document[2]
                document_date = document[3]
                document_download_path = document[5]
                output_path = str(document_download_path).replace('.pdf', '.xlsx')
                extraction_config = config_dict['extraction_config_path']
                logging.info(f"Going to extract for {document_name} with date - {document_date}")
                if 'form 6' in str(document_name).lower():
                    form6_run_check, form15_date = form_check(db_config, config_dict, registration_no, document_date)
                    if form6_run_check == 'true' or form15_date is None:
                        logging.info(f"Form 6 greater than form 15 so running form 6")
                        form6_extraction = form6_main(db_config, config_dict, document_download_path, output_path, registration_no, document_date, extraction_config)
                        if form6_extraction:
                            logging.info(f"Successfully extracted for {document_name}")
                            form15_percentage_holding_update = update_form15_percentage_holding(db_config, registration_no)
                            if form15_percentage_holding_update:
                                logging.info(f"Successfully updated form15 percentage holding")
                            update_extraction_status(db_config, document_id, registration_no)
                    else:
                        if str(form15_date).strip() == str(document_date).strip():
                            logging.info(f"Going for same date logic")
                            same_date_check = form6_same_date_check(db_config, registration_no, config_dict, document_download_path)
                            if same_date_check:
                                logging.info(f"Form 6 issue date is greater than annual date so running form 6")
                                form6_extraction = form6_main(db_config, config_dict, document_download_path,
                                                              output_path, registration_no, document_date,
                                                              extraction_config)
                                if form6_extraction:
                                    logging.info(f"Successfully extracted for {document_name}")
                                    update_extraction_status(db_config, document_id, registration_no)
                            else:
                                logging.info(f"Not running Form 6 as annual date is less than issue date")
                                update_extraction_needed_status_to_n(db_config, document_id, registration_no)
                        else:
                            logging.info(f"Not running Form 6 as it is not filed greater than form 15")
                            update_extraction_needed_status_to_n(db_config, document_id, registration_no)
                elif 'form 10' in str(document_name).lower():
                    form10_extraction = form10_main(db_config, config_dict, document_download_path, output_path, registration_no, extraction_config)
                    if form10_extraction:
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
                elif 'form 15' in str(document_name).lower():
                    form15_extraction = form15_main(db_config, config_dict, document_download_path, output_path, registration_no, extraction_config)
                    if form15_extraction:
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
                elif 'form 20' in str(document_name).lower():
                    form20_check, form15_date = form_check(db_config, config_dict, registration_no, document_date)
                    if form20_check == 'true' or str(form15_date).strip() == str(document_date).strip() or form15_date is None:
                        logging.info(f"Form 20 greater than form 15 so running form 6")
                        form20_extraction = form20_main(db_config, config_dict, document_download_path, output_path, registration_no, extraction_config)
                        if form20_extraction:
                            logging.info(f"Successfully extracted for {document_name}")
                            update_extraction_status(db_config, document_id, registration_no)
                    else:
                        logging.info(f"Not running Form 20 as it is not filed greater than form 15")
                        update_extraction_needed_status_to_n(db_config, document_id, registration_no)
                elif 'form 40' in str(document_name).lower() or 'form 3' in str(document_name).lower() or 'form 13' in str(document_name).lower():
                    form40_3_13_extraction = form40and3and13_main(db_config, config_dict, document_download_path, output_path, registration_no, document_name, extraction_config)
                    if form40_3_13_extraction:
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
                elif 'financial' in str(document_name).lower():
                    split_status = get_split_status(db_config, registration_no, document_id)
                    if str(split_status).lower() != 'y':
                        header_keywords = str(config_dict['headers']).split(',')
                        field_keywords = str(config_dict['fields']).split(',')
                        content_keywords = str(config_dict['contents']).split(',')
                        # Get the directory where the document is located
                        temp_pdf_directory = os.path.dirname(document_download_path)
                        pdf_document_name = os.path.basename(document_download_path)
                        pdf_document_name = os.path.splitext(pdf_document_name)[0]  # Safe removal of extension
                        temp_pdf_path = f"split_{pdf_document_name}.pdf"  # Ensures .pdf is always added
                        # Construct the full path for the split PDF
                        split_pdf_path = os.path.join(temp_pdf_directory, temp_pdf_path)
                        split_pdf_path = split_pdf_path.replace('\\', '/')
                        is_split_successful = split_pdf_based_on_headers_and_fields(document_download_path, split_pdf_path, header_keywords, field_keywords, content_keywords)
                        if is_split_successful:
                            update_split_status_and_split_pdf_path(db_config, registration_no, document_id, split_pdf_path)
                    temp_pdf_directory = os.path.dirname(document_download_path)
                    pdf_document_name = os.path.basename(document_download_path)
                    pdf_document_name = str(pdf_document_name).replace('.pdf','')
                    temp_pdf_name_finance = 'temp_finance_' + pdf_document_name
                    if '.pdf' not in temp_pdf_name_finance:
                        temp_pdf_name_finance = temp_pdf_name_finance + '.pdf'
                    temp_pdf_path_finance = os.path.join(temp_pdf_directory, temp_pdf_name_finance)
                    finance_output_file_name = 'finance_' + pdf_document_name
                    if '.xlsx' not in finance_output_file_name:
                        finance_output_file_name = finance_output_file_name + '.xlsx'
                    finance_output_file_path = os.path.join(temp_pdf_directory, finance_output_file_name)
                    finance_status, profit_and_loss_status = get_financial_status(db_config, registration_no, document_id)
                    if str(finance_status).lower() != 'y':
                        finance_type='finance'
                        main_finance_extraction = finance_main(db_config, config_dict, document_download_path, registration_no,finance_type,temp_pdf_path_finance, finance_output_file_path,document_id)
                        if main_finance_extraction:
                            logging.info(f"Successfully extracted for assets and liabilities")
                            update_finance_status(db_config, registration_no, document_id)
                    else:
                        logging.info(f"Already extracted for assets and liabilities")
                    temp_pdf_name_pnl = 'temp_pnl_' + pdf_document_name
                    if '.pdf' not in temp_pdf_name_pnl:
                        temp_pdf_name_pnl = temp_pdf_name_pnl + '.pdf'
                    temp_pdf_path_pnl = os.path.join(temp_pdf_directory, temp_pdf_name_pnl)
                    pnl_output_file_name = 'pnl_' + pdf_document_name
                    if '.xlsx' not in pnl_output_file_name:
                        pnl_output_file_name = pnl_output_file_name + '.xlsx'
                    pnl_output_path = os.path.join(temp_pdf_directory, pnl_output_file_name)
                    if str(profit_and_loss_status).lower() != 'y':
                        finance_type = 'pnl'
                        pnl_extraction = finance_main(db_config, config_dict, document_download_path, registration_no, finance_type,temp_pdf_path_pnl, pnl_output_path, document_id)

                        if pnl_extraction:
                            logging.info(f"Successfully extracted Profit and Loss")
                            update_pnl_status(db_config, registration_no, document_id)
                    else:
                        logging.info(f"Already extracted Profit and Loss")
                    updated_finance_status, updated_pnl_status = get_financial_status(db_config, registration_no, document_id)
                    if str(updated_finance_status).lower() == 'y' and str(updated_pnl_status).lower() == 'y':
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
            except Exception as e:
                logging.error(f"Error {e} occurred while extracting for file - {document_name} at path - {document_download_path}")
                error_count += 1
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"File - {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        try:
            get_holding_entities(db_config,registration_no,config_dict)
        except Exception as e:
            logging.error(f"Error in fetching holding entities {e}")
        try:
            split_address(registration_no, config_dict, db_config)
        except Exception as e:
            logging.error(f"Error in splitting address")
            tb = traceback.extract_tb(e.__traceback__)
            for frame in tb:
                if frame.filename == __file__:
                    errors.append(f"File - {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
            raise Exception(errors)
    except Exception as e:
        logging.error(f"Error occurred while extracting for Reg no - {registration_no}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        logging.error("\n".join(errors))
        raise Exception(errors)
    else:
        pending_files = extraction_pending_files(db_config, registration_no)
        if len(pending_files) <= 2:
            return True
        else:
            logging.error("\n".join(errors))
            raise Exception("\n \n".join(errors))


def json_loader_and_tables(db_config, config_excel_path, registration_no, receipt_no, config_dict, database_id):
    errors = []
    try:
        config_json_file_path = config_dict['config_json_file_path']
        root_path = config_dict['Root path']
        sheet_name = 'JSON_Loader_SQL_Queries'
        final_email_table = None
        form13_file_table = None
        no_of_form13 = None
        financial_table = None
        tags_table = None
        json_loader_status, json_file_path, json_nodes = json_loader(db_config, config_json_file_path, registration_no, root_path, config_excel_path, sheet_name, receipt_no)
        if json_loader_status:
            order_sheet_name = "JSON Non-LLP Order"
            config_dict_order, status = create_main_config_dictionary(config_excel_path, order_sheet_name)
            for json_node in json_nodes:
                try:
                    json_order_status = order_json(config_dict_order, json_node, json_file_path)
                    if json_order_status:
                        logging.info(f"Successfully ordered json for {json_node}")
                except Exception as e:
                    logging.error(f"Error occurred while ordering for {json_node} {e}")
            final_email_table = final_table(db_config, registration_no, database_id)
            form13_file_table, no_of_form13 = form13_table(db_config, registration_no)
            financial_table = financials_table(db_config, registration_no)
            tags_table = new_tags_table(db_config, registration_no)
    except Exception as e:
        logging.error(f"Exception occurred while generating json loader {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception('\n'.join(errors))
    else:
        return True, final_email_table, json_file_path, form13_file_table, no_of_form13, financial_table,tags_table

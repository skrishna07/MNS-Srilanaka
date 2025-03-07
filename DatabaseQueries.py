import mysql.connector
from PythonLogging import setup_logging
import logging
import os
from datetime import datetime
import json
import traceback
import requests


def fetch_orders_to_extract_data(db_config):
    try:
        setup_logging()
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        pending_order_query = "select receipt_no,registration_no,company_name,id,workflow_status from orders where process_status = 'InProgress' and LOWER(workflow_status) in ('extraction_pending','loader_pending')"
        logging.info(pending_order_query)
        cursor.execute(pending_order_query)
        pending_order_results = cursor.fetchall()
        cursor.close()
        connection.close()
        return pending_order_results
    except Exception as e:
        logging.error(f"Exception {e} occurred")
        return []


def get_db_credentials(config_dict):
    host = config_dict['Host']
    db_user = config_dict['User']
    password = config_dict['Password']
    database = config_dict['Database']
    db_config = {
        "host": host,
        "user": db_user,
        "password": password,
        "database": database,
        "connect_timeout": 6000,
        "charset": 'utf8mb4'
    }
    return db_config


def update_locked_by(dbconfig, registration_id):
    setup_logging()
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    try:
        user = os.environ.get('SystemName')
        update_locked_query = f"update orders set locked_by = '{user}' where id ='{registration_id}'"
        cursor.execute(update_locked_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def update_locked_by_empty(dbconfig, registration_id):
    setup_logging()
    connection = mysql.connector.connect(**dbconfig)
    cursor = connection.cursor()
    try:
        update_locked_query = f"update orders set locked_by = '' where id ='{registration_id}'"
        cursor.execute(update_locked_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def update_modified_date(db_config, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        current_date = datetime.now()
        today_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
        update_locked_query = f"update orders set modified_date = '{today_date}' where id = {database_id}"
        logging.info(update_locked_query)
        cursor.execute(update_locked_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def update_workflow_status(db_config, reg_id, status):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"Update orders set workflow_status = '{status}' where id = {reg_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating workflow status {e}")


def update_process_status(db_config, database_id, status):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        query = f"Update orders set process_status = '{status}' where id = {database_id}"
        logging.info(query)
        cursor.execute(query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error in updating process status {e}")


def update_retry_count(db_config, registration_no, retry_counter, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_retry_counter_query = f"update orders set retry_counter = {retry_counter} where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_retry_counter_query)
        cursor.execute(update_retry_counter_query)
        connection.commit()
    except Exception as e:
        print(f"Exception occurred while updating retry counter by {e}")
    finally:
        cursor.close()
        connection.close()


def get_retry_count(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        retry_counter_query = f"select retry_counter from orders where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(retry_counter_query)
        cursor.execute(retry_counter_query)
        result = cursor.fetchone()[0]
        logging.info(f"Retry count {result}")
        return result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def get_documents_to_extract(db_config, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        extract_documents_query = f"select * from documents where registration_no = '{registration_no}' and document_extraction_status = 'Pending' and document_extraction_needed = 'Y' ORDER BY (CASE WHEN document_name LIKE '%Form 15%' THEN 0 ELSE 1 END),STR_TO_DATE(document_date, '%d-%m-%Y') DESC"
        logging.info(extract_documents_query)
        cursor.execute(extract_documents_query)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error while fetching results {e}")
        raise Exception(e)
    else:
        return result


def extraction_pending_files(db_config, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        pending_files_query = f"select * from documents where registration_no = '{registration_no}' and document_extraction_status = 'Pending' and document_extraction_needed = 'Y'"
        logging.info(pending_files_query)
        cursor.execute(pending_files_query)
        pending_files = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error in fetching number of pending files {e}")
    else:
        return pending_files


def update_extraction_status(db_config, document_id, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set document_extraction_status = 'Success' where registration_no = '{registration_no}' and id = {document_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_database_single_value(db_config, table_name, registration_no_column_name, registration_no, column_name, column_value):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}'".format(table_name, registration_no_column_name, registration_no)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}'".format(table_name, column_name,
                                                                                      column_value, registration_no_column_name,
                                                                                      registration_no)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}) VALUES ('{}', '{}')".format(table_name, registration_no_column_name,
                                                                                      column_name,
                                                                                      registration_no,
                                                                                      column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def insert_datatable_with_table_director(config_dict, db_config, sql_table_name, column_names_list, df_row):
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
            update_query = f'''UPDATE {sql_table_name} SET {', '.join([f'{col} = "{str(result_dict[col])}"' for col in column_names_list])} 
                            WHERE {registration_column_name} = "{registration_no}" AND {name_column_name} = "{name}"'''
            logging.info(update_query)
            db_cursor.execute(update_query)
            logging.info(f"Data row values are saved in table '{sql_table_name}' with \n {df_row}")
    db_cursor.close()
    db_connection.close()


def update_database_single_value_financial(db_config, table_name, registration_no_column_name, registration_no, column_name, column_value, year, nature):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' and {}='{}' and {}='{}'".format(table_name, registration_no_column_name, registration_no, 'year', year, 'nature', nature)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}' AND {}='{}'".format(table_name, column_name,
                                                                                      column_value, registration_no_column_name,
                                                                                      registration_no,
                                                                                      'Year',
                                                                                      year,
                                                                                      'nature',
                                                                                       nature)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}, {}) VALUES ('{}', '{}', '{}')".format(table_name, registration_no_column_name,
                                                                                      column_name,
                                                                                      'nature',
                                                                                      registration_no,
                                                                                      column_value,
                                                                                      nature)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def form_check(db_config, config_dict, registration_no, document_date):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = str(config_dict['Form6_check_query']).format(document_date, registration_no)
        logging.info(query)
        cursor.execute(query)
        result = cursor.fetchone()
        logging.info(result)
        status = result[0]
        form15_date = result[1]
        return status, form15_date
    except Exception as e:
        logging.info(f"Error occurred while checking form 6 {e}")
        return None


def update_form_extraction_status(db_config, registration_no, config_dict):
    errors = []
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query_form15 = str(config_dict['form15_extraction_needed_update_query']).format(registration_no, registration_no)
        logging.info(update_query_form15)
        cursor.execute(update_query_form15)
        update_query_form10 = str(config_dict['form10_extraction_needed_update_query']).format(registration_no)
        logging.info(update_query_form10)
        cursor.execute(update_query_form10)
        update_query_form40 = str(config_dict['form40_extraction_needed_update_query']).format(registration_no, registration_no)
        logging.info(update_query_form40)
        cursor.execute(update_query_form40)
        update_query_form20 = str(config_dict['form20_extraction_needed_update_query']).format(registration_no, registration_no)
        logging.info(update_query_form20)
        cursor.execute(update_query_form20)
        update_query_form6 = str(config_dict['form6_extraction_needed_update_query']).format(registration_no, registration_no)
        logging.info(update_query_form6)
        cursor.execute(update_query_form6)
        update_query_financial = str(config_dict['financial_update_query']).format(registration_no)
        logging.info(update_query_financial)
        cursor.execute(update_query_financial)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating form extraction status {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File - {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        return True


def update_extraction_needed_status_to_n(db_config, document_id, registration_no):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set document_extraction_needed = 'N' where registration_no = '{registration_no}' and id = {document_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_bot_comments_empty(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_comments_query = f"update orders set bot_comments = '',retry_counter = '',exception_type = '' where registration_no = '{registration_no}' and id ='{database_id}'"
        cursor.execute(update_comments_query)
        connection.commit()
    except Exception as e:
        logging.error(f"Exception occurred while updating locked by {e}")
    finally:
        cursor.close()
        connection.close()


def get_financial_status(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        retry_counter_query = f"select financial_status,profit_and_loss_status from documents where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(retry_counter_query)
        cursor.execute(retry_counter_query)
        result = cursor.fetchall()[0]
        financial_result = result[0]
        profit_and_loss_result = result[1]
        logging.info(f"financial status {result}")
        return financial_result, profit_and_loss_result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def update_finance_status(db_config, registration_no, database_id):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set financial_status = 'Y' where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_pnl_status(db_config, registration_no, database_id):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        update_query = f"update documents set profit_and_loss_status = 'Y' where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(update_query)
        cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error updating extraction status {e}")


def update_database_single_value_with_one_column_check(db_config, table_name, registration_no_column_name, registration_no, column_name, column_value, column_to_check, value_to_check):
    setup_logging()
    db_connection = mysql.connector.connect(**db_config)
    db_cursor = db_connection.cursor()
    json_dict = json.loads(column_value)
    num_elements = len(json_dict)
    if num_elements == 1:
        first_key = next(iter(json_dict))
        first_value = json_dict[first_key]
        column_value = first_value
    else:
        column_value = json.dumps(json_dict)

    # check if there is already entry with cin
    query = "SELECT * FROM {} WHERE {} = '{}' AND {} = '{}'".format(table_name, registration_no_column_name, registration_no, column_to_check, value_to_check)
    logging.info(query)
    try:
        db_cursor.execute(query)
    except mysql.connector.Error as err:
        logging.info(err)
    result = db_cursor.fetchall()
    # logging.info(result)

    # if cin value already exists
    if len(result) > 0:
        update_query = "UPDATE {} SET {} = '{}' WHERE {} = '{}' AND {} = '{}'".format(table_name, column_name,
                                                                                      column_value, registration_no_column_name,
                                                                                      registration_no,column_to_check,value_to_check)
        logging.info(update_query)
        db_cursor.execute(update_query)
        logging.info("Updating")

    # if cin value doesn't exist
    else:
        insert_query = "INSERT INTO {} ({}, {}) VALUES ('{}', '{}')".format(table_name, registration_no_column_name,
                                                                                      column_name,
                                                                                      registration_no,
                                                                                      column_value)
        logging.info(insert_query)
        db_cursor.execute(insert_query)
        logging.info("Inserting")
    db_connection.commit()
    db_cursor.close()
    db_connection.close()


def get_legal_name_form15(db_config, registration_no):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        legal_name_query = f"select legal_name from Company where registration_no = '{registration_no}'"
        logging.info(legal_name_query)
        cursor.execute(legal_name_query)
        result = cursor.fetchone()[0]
        return result
    except Exception as e:
        logging.info(f"Exception occurred while updating retry counter by {e}")
        return None
    finally:
        cursor.close()
        connection.close()


def update_completed_status_api(orderid, config_dict):
    setup_logging()
    try:
        url = config_dict['update_api_url']

        payload = json.dumps({
            "receiptnumber": orderid,
            "status": 2
        })
        headers = {
            'Authorization': config_dict['Authorization'],
            'Content-Type': 'application/json',
            'Cookie': config_dict['Cookie']
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        logging.info(response.text)
    except Exception as e:
        logging.info(f"Error in updating status in API {e}")
        return False
    else:
        return True


def update_end_time(db_config, registration_no, database_id):
    try:
        setup_logging()
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        check_query = f"SELECT end_time FROM orders WHERE registration_no = '{registration_no}' and id = {database_id}"
        cursor.execute(check_query)
        result = cursor.fetchone()
        if result is not None and result[0] is None:
            update_query = f"update orders set end_time = '{current_datetime}' where registration_no = '{registration_no}' and id = {database_id}"
            logging.info(update_query)
            cursor.execute(update_query)
        cursor.close()
        connection.close()
    except Exception as e:
        logging.info(f"Error updating end time {e}")


def remove_text_before_marker(text, marker):
    index = text.find(marker)
    if index != -1:
        return text[index + len(marker):]
    return text


def remove_string(text, string_to_remove):
    if string_to_remove in text:
        text = text.replace(string_to_remove, "")
    return text

def get_split_status(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        split_status_query = f"select split_status from documents where registration_no = '{registration_no}' and id = {database_id}"
        logging.info(split_status_query)
        cursor.execute(split_status_query)
        result = cursor.fetchone()[0]
        logging.info(f"Split status retrieved successfully for registration_no={registration_no}, id={database_id}: {result[0]}")
        return result
    except Exception as e:
        logging.error(f"An unexpected exception occurred: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def update_split_status_and_split_pdf_path(db_config, registration_no, database_id, split_pdf_path):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        update_split_status_path__query = f"UPDATE documents SET split_status = 'Y', split_pdf_path = '{split_pdf_path}' WHERE registration_no = '{registration_no}' AND id = {database_id}"
        logging.info(update_split_status_path__query)
        cursor.execute(update_split_status_path__query)
        connection.commit()
    except Exception as e:
        print(f"Exception occurred while updating split_status by {e}")
    finally:
        cursor.close()
        connection.close()

def get_split_pdf_path(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        query = (
            f"SELECT split_status, split_pdf_path, pdf_to_excel_conversion_status,excel_path "  
            f"FROM documents WHERE registration_no = '{registration_no}' AND id = {database_id}"
        )
        logging.info(query)
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            split_status = result[0]  # Assign split_status
            split_pdf_path = result[1]  # Assign split_pdf_path
            pdf_to_excel_conversion_status = result[2]  # Assign pdf_to_excel_conversion_status
            excel_file_path = result[3]

            logging.info(f"Fetched values - split_status: {split_status}, split_pdf_path: {split_pdf_path}, "
                         f"pdf_to_excel_conversion_status: {pdf_to_excel_conversion_status}")
            return split_status,split_pdf_path,pdf_to_excel_conversion_status,excel_file_path
        else:
            logging.info("No record found for the given registration_no and database_id.")
            return None
    except Exception as e:
        logging.error(f"Exception occurred while fetching document status: {e}")
        return None
    finally:
        cursor.close()
        connection.close()

def update_excel_status_and_path(db_config, registration_no, database_id, excel_path):
    """
    Update pdf_to_excel_conversion_status to 'Y' and set the excel_path in the documents table.
    """
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Update pdf_to_excel_conversion_status to 'Y' and set the excel_path
        update_query = (
            f"UPDATE documents SET pdf_to_excel_conversion_status = 'Y', excel_path = '{excel_path}' "
            f"WHERE registration_no = '{registration_no}' AND id = {database_id}"
        )
        logging.info(update_query)
        cursor.execute(update_query)
        connection.commit()  # Commit the update

        logging.info(f"Updated pdf_to_excel_conversion_status to 'Y' and excel_path to {excel_path}")
        return True  # Return True to indicate the update was successful
    except Exception as e:
        logging.error(f"Exception occurred while updating document status: {e}")
        return False  # Return False if an error occurred
    finally:
        cursor.close()
        connection.close()

def insert_new_tags(db_config, registration_no, database_id, all_tags_data, column_name):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Serialize `all_tags_data` to JSON if it is a list or dictionary
        if isinstance(all_tags_data, (list, dict)):
            all_tags_data = json.dumps(all_tags_data)

        # Construct the dynamic update query with placeholders for parameters
        update_query = f"UPDATE documents SET {column_name} = %s WHERE registration_no = %s AND id = %s"

        # Log the query for debugging purposes (omit actual data for safety)
        logging.info(f"Executing query: {update_query} with data: {column_name}, {registration_no}, {database_id}")

        # Execute the query with parameterized data
        cursor.execute(update_query, (all_tags_data, registration_no, database_id))
        connection.commit()  # Commit the changes

        logging.info(
            f"Successfully updated column '{column_name}' with new tags data for registration_no={registration_no}, id={database_id}")
        return True  # Return True to indicate success
    except mysql.connector.Error as db_error:
        logging.error(f"Database error occurred: {db_error}")
        return False  # Return False in case of a database error
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")
        return False  # Return False in case of a general error
    finally:
        cursor.close()
        connection.close()
        logging.info("Database connection closed.")

def get_new_tags(db_config, registration_no, database_id):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    try:
        # Create the SELECT query
        query = (
            f"SELECT finance_new_tags, pnl_new_tags "
            f"FROM documents WHERE registration_no = '{registration_no}' AND id = {database_id}"
        )
        logging.info(f"Executing query: {query}")
        cursor.execute(query)
        result = cursor.fetchone()

        # If data is found, return the two columns; otherwise, return None for both
        if result:
            finance_new_tags = result[0]  # finance_new_tags from the query result
            pnl_new_tags = result[1]      # pnl_new_tags from the query result
            logging.info(f"Retrieved tags: finance_new_tags={finance_new_tags}, pnl_new_tags={pnl_new_tags}")
            return finance_new_tags, pnl_new_tags
        else:
            logging.warning(f"No data found for registration_no={registration_no} and id={database_id}")
            return None, None  # Return None if no data is found

    except Exception as e:
        logging.error(f"Exception occurred while retrieving new tags: {e}")
        return None, None  # Return None if an error occurred

    finally:
        cursor.close()
        connection.close()
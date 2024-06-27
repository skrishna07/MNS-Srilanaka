import json
import mysql.connector
import logging
from PythonLogging import setup_logging
from OpenAI import split_openai


def split_address(registration_no,config_dict,db_config):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    address_query = f"select address from authorized_signatories where registration_no = '{registration_no}'"
    logging.info(address_query)
    cursor.execute(address_query)
    address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    prompt = config_dict['Prompt']
    for address in address_list:
        address_to_split = address[0]
        address_to_split = address_to_split.replace("'", "").replace('"', "")
        logging.info(address_to_split)
        if str(address_to_split).lower() != 'null' and address_to_split is not None:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            connection.autocommit = True
            splitted_address = split_openai(address_to_split,prompt)
            update_query = f"update authorized_signatories set splitted_address = '{splitted_address}' where registration_no = '{registration_no}' and address = '{address_to_split}'"
            logging.info(update_query)
            cursor.execute(update_query)
            cursor.close()
            connection.close()

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    previous_address_query = f"select previous_address from previous_address where registration_no = '{registration_no}'"
    logging.info(previous_address_query)
    cursor.execute(previous_address_query)
    previous_address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    for previous_address in previous_address_list:
        previous_address_to_split = previous_address[0]
        previous_address_to_split = previous_address_to_split.replace("'", "").replace('"', "")
        if str(previous_address_to_split).lower() != 'null' and previous_address_to_split is not None:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            connection.autocommit = True
            previous_splitted_address = split_openai(previous_address_to_split, prompt)
            try:
                previous_splitted_address = eval(previous_splitted_address)
            except Exception as e:
                previous_splitted_address = json.loads(previous_splitted_address)
            city = previous_splitted_address['city']
            state = previous_splitted_address['state']
            pincode = previous_splitted_address['pincode']
            try:
                previous_splitted_address = str(previous_splitted_address).replace("'",'"')
            except:
                pass
            update_query = f"update previous_address set previous_splitted_address	 = '{previous_splitted_address}',city = '{city}',state = '{state}', pincode = '{pincode}' where registration_no = '{registration_no}' and previous_address = '{previous_address_to_split}'"
            logging.info(update_query)
            cursor.execute(update_query)
            cursor.close()
            connection.close()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    registered_address_query = f"select registered_full_address from Company where registration_no = '{registration_no}'"
    logging.info(registered_address_query)
    cursor.execute(registered_address_query)
    registered_address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    for registered_address in registered_address_list:
        registered_address_to_split = registered_address[0]
        registered_address_to_split = registered_address_to_split.replace("'", "").replace('"', "")
        if str(registered_address_to_split).lower() != 'null' and registered_address_to_split is not None:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            connection.autocommit = True
            registered_splitted_address = split_openai(registered_address_to_split, prompt)
            try:
                registered_splitted_address = eval(registered_splitted_address)
            except Exception as e:
                registered_splitted_address = json.loads(registered_splitted_address)
            address_line1 = registered_splitted_address['address_line1']
            address_line2 = registered_splitted_address['address_line2']
            city = registered_splitted_address['city']
            state = registered_splitted_address['state']
            pincode = registered_splitted_address['pincode']
            try:
                registered_splitted_address = str(registered_splitted_address).replace("'", '"')
            except:
                pass
            update_query = f"update Company set registered_splitted_address	 = '{registered_splitted_address}',registered_city = '{city}',registered_state = '{state}',registered_pincode = '{pincode}',registered_address_line1 = '{address_line1}',registered_address_line2 = '{address_line2}' where registration_no = '{registration_no}' and registered_full_address = '{registered_address_to_split}'"
            logging.info(update_query)
            cursor.execute(update_query)
            cursor.close()
            connection.close()

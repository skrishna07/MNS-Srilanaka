import logging
import mysql.connector
from PythonLogging import setup_logging
from OpenAI import split_openai
from AmazonOCR import extract_text_from_pdf
import json
from datetime import datetime


def form6_same_date_check(db_config, registration_no, config_dict, form6_pdf_path):
    setup_logging()
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        connection.autocommit = True
        annual_return_date_query = f"select date_of_annual_return from Company where registration_no = '{registration_no}'"
        logging.info(annual_return_date_query)
        cursor.execute(annual_return_date_query)
        annual_return_date = cursor.fetchone()[0]
        annual_return_date = str(annual_return_date).strip()
        cursor.close()
        connection.close()
        form6_text = extract_text_from_pdf(form6_pdf_path)
        output = split_openai(form6_text, config_dict['form6_check_prompt'])
        logging.info(output)
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        form6date = output['date_of_issue']
        form6date = datetime.strptime(form6date, '%Y-%m-%d')
        annual_return_date = datetime.strptime(annual_return_date, '%Y-%m-%d')
        if form6date > annual_return_date:
            logging.info(f"Running as issue date of form 6 {form6date} filed greater than Form 15 Annual return Date {annual_return_date}")
            return True
        else:
            logging.info(f"Not running as issue date of form 6 {form6date} not filed greater than Form 15 Annual return Date {annual_return_date}")
            return False
    except Exception as e:
        logging.error(f"Error occurred in checking form 6 same date condition {e}")
        return None

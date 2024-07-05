import logging
from PythonLogging import setup_logging
from SendEmail import send_email


def exception_handler(e, registration_no, config_dict, receipt_no, company_name):
    setup_logging()
    logging.error(f"Exception occurred {e}")

    exception_subject = str(config_dict['Exception_subject']).format(registration_no, receipt_no)
    exception_message = str(config_dict['Exception_message']).format(registration_no, receipt_no, company_name, e)
    exception_mails = str(config_dict['Exception_mails']).split(',')
    send_email(config_dict, exception_subject, exception_message, exception_mails, None)

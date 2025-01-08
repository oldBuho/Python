#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
# email
import smtplib
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# handling of function parameters
import argparse
# sys to import config
import sys

try:
    reload(sys)
    sys.setdefaultencoding('utf-8')
except: pass

def get_config_testCompany():
    try:
        f = open('/home/ec2-user/testCompany/config/testCompany.json', encoding='utf-8')
    except:
        f = open('/home/ec2-user/testCompany/config/testCompany.json')
    parse = json.load(f)
    f.close()
    return parse

def send_email(subject, body, recipients = get_config_testCompany()["email"]["RECIPIENTS_desa"] ):
    # get testCompany config
    config_testCompany = get_config_testCompany()
    HOST = config_testCompany["email"]["HOST"]
    PORT = int(config_testCompany["email"]["PORT"])
    USERNAME_SMTP = config_testCompany["email"]["USERNAME_SMTP"]
    PASSWORD_SMTP = config_testCompany["email"]["PASSWORD_SMTP"]
    SENDER = config_testCompany["email"]["SENDER"]
    SENDERNAME = config_testCompany["email"]["SENDERNAME"]
    # RECIPIENT = see function default parameter

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))

    # Try to send the message.
    try:
        # The email body for recipients with non-HTML email clients.
        BODY_TEXT = "testCompany monitoring\r\n"+body
        # The HTML body of the email.
        BODY_HTML = """<html>
        <head></head>
        <body>
        <h1>testCompany Monitoreo y Control</h1>
        <p>"""+body+""" </p>
        </body>
        </html>
        """

        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(BODY_TEXT, 'plain')
        part2 = MIMEText(BODY_HTML, 'html')

        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part1)
        msg.attach(part2)

        server = smtplib.SMTP(HOST, PORT)
        server.ehlo()
        server.starttls()
        #stmplib docs recommend calling ehlo() before & after starttls()
        server.ehlo()
        server.login(USERNAME_SMTP, PASSWORD_SMTP)
        server.sendmail(SENDER, recipients, msg.as_string())
        server.close()
    # Display an error message if something goes wrong.
    except Exception as e:
        print ("Error: ", e)
    else:
        print ("Email sent!")

def get_app_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--function", help="The function you want to execute")
    parser.add_argument("-p1", "--parameter_1", help="First parameter to pass to the function", default='Test subject')
    parser.add_argument("-p2", "--parameter_2", help="Second parameter to pass to the function", default='Test email body')
    parser.add_argument("-p3", "--parameter_3", help="Third parameter to pass to the function")
    parser.add_argument("-p4", "--parameter_4", help="Forth parameter to pass to the function")
    return parser.parse_args()

if __name__ == '__main__':
    # message to users
    # get args
    app_args = get_app_args()

    if app_args.function == 'send_email':
        # Execute function
        send_email(app_args.parameter_1, app_args.parameter_2, app_args.parameter_3)
    #elif app_args.function == 'other_function':
        # execute other function
    else:
        print("""
        testCompany users:
            get_config_testCompany() => config json
            retrieve testCompany config data (stored in file '/home/ec2-user/testCompany/config/testCompany.json')

            send_email(Subject, Body) 
            to send email notifications

            to send notifications from the command line:
            python testCompany_utils.py --function=send_email --parameter_1=\"Email Suject\" --parameter_2=\"Email Body\"
        """)

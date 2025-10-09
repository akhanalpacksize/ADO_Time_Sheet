import logging
import os.path
import smtplib
from base64 import b64encode
from copy import deepcopy, error
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas
import requests

from config.env import GRANT_TYPE, AUTH_URL, CLIENT_ID, CLIENT_SECRET, SMTP_SERVER,SMTP_PORT,SMTP_USERNAME, SMTP_PASSWORD,sender_email, receiver_emails

from logger_config import setup_logging

# Setup logging
setup_logging(module_name="Fetch_utils_log")

logger = logging.getLogger(__name__)


def send_email(subject, body_data, sender, recipients):
    # Retrieve the file path of the currently executing script
    script_path = os.path.abspath(__file__)
    project_name = os.path.basename(os.path.dirname(script_path))

    # Email body template
    body_template = """\
<html>
<body>
<p>Greetings Team,</p>

<p>An error occurred in the <b>{}</b> that requires immediate attention.<br>
Here are the key details:</p>

<p>Error Message: <b>{}</b></p>
<p>Timestamp: <b>{}</b></p>

<p>Our team is actively resolving this issue promptly and is fully committed to minimizing its impact on our operations.</p>

<p>We apologize for any inconvenience caused and appreciate your cooperation during this time.</p>

<p>Best regards,<br>
<i>Information Systems Team</i></p>
</body>
</html>
"""
    # Format the email body
    body = body_template.format(project_name, *body_data)

    # Create the email message
    message = MIMEMultipart()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = ", ".join(recipients)  # Join multiple emails into a string
    message.attach(MIMEText(body, "html"))

    try:
        # Connect to the SMTP server and send the email
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  # Encrypt the connection
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(sender, recipients, message.as_string())
            print("Email sent successfully to multiple recipients.")
    except Exception as e:
        print(f"Failed to send email. Error: {e}")

def send_email_error(error_message):
    # Send error email notification
    subject = "Error Report"
    body_data = [error_message, datetime.utcnow().isoformat() + 'Z']
    send_email(subject, body_data, sender_email, receiver_emails)


def token_is_expired(expiration_time_sec):
    # Check if the token is expired
    return datetime.utcnow() >= expiration_time_sec


def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for index, right_row in enumerate(right):
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows


def flatten_list(data):
    for index, elem in enumerate(data):
        elem['Rank'] = index + 1
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def json_to_dataframe(data_in):
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                if value and "\r\n" in str(value):
                    value = value.replace("\r\n", '-')

                if prev_heading:
                    rows = cross_join(rows, flatten_json(value, prev_heading + '_' + key))
                else:
                    rows = cross_join(rows, flatten_json(value, key))

        elif isinstance(data, list):
            rows = []
            for item in data:
                [rows.append(elem) for elem in flatten_list(flatten_json(item, prev_heading))]
        else:
            rows = [{prev_heading: data}]
        return rows

    return pandas.DataFrame(flatten_json(data_in), dtype=str)


def get_access_token():
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode()
    }

    payload = {
        "grant_type": GRANT_TYPE,
    }

    try:
        response = requests.post(AUTH_URL, data=payload, headers=auth_headers)
        response.raise_for_status()
        response_json = response.json()
        access_token = response_json['access_token']
        return access_token

    except requests.exceptions.HTTPError as e:
        error_message = e.response.json().get("error_description", "Unknown error")
        raise Exception(f"Authentication error: {e.response.status_code} - {error_message}")



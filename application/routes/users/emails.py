import os
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
from aiohttp import ClientError
from dotenv import load_dotenv
from fastapi.templating import Jinja2Templates

from application.aws_helper.helper import SES_CLIENT
from application.routes.users import emails_helper

load_dotenv()
FRONTEND_URL = os.getenv("FRONTEND_URL")
templates = Jinja2Templates(directory="application/routes/users/templates")


def render_template(template_name: str, **kwargs) -> str:
    template = templates.get_template(template_name)
    return template.render(**kwargs)


def send_email_to_activate_user(recipient: str, qrcode_image: str, password: str):
    # Create a new SES resource
    ses = SES_CLIENT
    sender = "admin@claxonbusinesssolutions.com"

    # Try to send the email
    try:
        response = ses.send_email(
            Source=sender,
            Destination={
                "ToAddresses": [
                    recipient,
                ],
            },
            Message={
                "Subject": {"Data": "REGISTRATION CONFIRMATION"},
                "Body": {
                    "Html": {
                        "Data": emails_helper.activate_user_html(
                            password, "http://budgeting.claxonfintech.com", qrcode_image
                        )
                    }
                },
            },
        )
    except ClientError as e:
        raise e
    else:
        return response


def send_email_to_reset_password(recipient: str, token: str):
    # Create a new SES resource
    ses = SES_CLIENT
    sender = "admin@claxonbusinesssolutions.com"

    # Try to send the email
    try:
        response = ses.send_email(
            Source=sender,
            Destination={
                "ToAddresses": [
                    recipient,
                ],
            },
            Message={
                "Subject": {"Data": "REGISTRATION CONFIRMATION"},
                "Body": {
                    "Html": {
                        "Data": emails_helper.email_to_change_password(
                            token=token, url="http://budgeting.claxonfintech.com"
                        )
                    }
                },
            },
        )
    except ClientError as e:
        raise e
    else:
        return response

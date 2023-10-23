import os
from datetime import timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
from aiohttp import ClientError
from dotenv import load_dotenv
from fastapi.templating import Jinja2Templates

from application.auth import security
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
    access_token = security.create_access_token(data={"email": recipient})

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
                            password,
                            f"http://budgeting.claxonfintech.com/reset-password?access-token={access_token}",
                            qrcode_image,
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
    access_token = security.create_access_token(
        data={"email": recipient}, expires_delta=timedelta(hours=48)
    )

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
                            token=token,
                            url=f"http://budgeting.claxonfintech.com/reset-password?access-token={access_token}",
                        )
                    }
                },
            },
        )
    except ClientError as e:
        raise e
    else:
        return response

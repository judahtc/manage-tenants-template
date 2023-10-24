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


def send_email_to_activate_user(
    recipient: str, qrcode_image: str, password: str, token: str
):
    # Create a new SES resource
    ses = SES_CLIENT
    sender = "admin@claxonbusinesssolutions.com"

    account_activation_email = render_template(
        "activate_user.html",
        url=f"http://budgeting.claxonfintech.com/reset-password?access-token={token}",
        qrcode_image=qrcode_image,
    )

    # account_activation_email =  emails_helper.activate_user_html(
    #                         f"http://budgeting.claxonfintech.com/reset-password?access-token={token}",
    #                         qrcode_image,
    #                     )

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
                "Body": {"Html": {"Data": account_activation_email}},
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

    reset_password_email = render_template(
        "reset_password.html",
        url=f"http://budgeting.claxonfintech.com/reset-password?access-token={token}",
    )

    # reset_password_email = emails_helper.email_to_change_password(
    #                         url=f"http://budgeting.claxonfintech.com/reset-password?access-token={token}",
    #                     )

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
                "Body": {"Html": {"Data": reset_password_email}},
            },
        )
    except ClientError as e:
        raise e
    else:
        return response

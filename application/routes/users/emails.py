from aiohttp import ClientError
import boto3
from application.aws_helper.helper import SES_CLIENT
from application.routes.users import emails_helper
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


def send_email(recipient: str, qrcode_image: str, password: str):
    # Create a new SES resource
    ses = SES_CLIENT
    sender = 'admin@claxonbusinesssolutions.com'

    # Try to send the email
    try:
        response = ses.send_email(
            Source=sender,
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Subject': {
                    'Data': "REGISTRATION CONFIRMATION"
                },
                'Body': {
                    'Html': {
                        'Data': emails_helper.activate_user_html(password, "http://budgeting.claxonfintech.com", qrcode_image)
                    }
                }
            }
        )
    except ClientError as e:
        # Print any error messages
        raise e
    else:
        return response

# Usage example

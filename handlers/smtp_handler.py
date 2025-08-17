import email.utils
import smtplib

from datetime import datetime

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dependencies.configuration import Configuration
from dependencies.logger import logger


class SMTPHandler:

    @staticmethod
    def send_aws_ses_exception(txn_id=None, error_message='', subject=None, recipient_list= None):
        """ This function sends mail. Input required: customer id, exception message replaces mandrill"""
        ENV = Configuration.ENV

        try:
            # Replace sender@example.com with your "From" address.
            # This address must be verified.
            sender_email = Configuration.SENDER_EMAIL

            # Replace recipient@example.com with a "To" address. If your account
            # is still in the sandbox, this address must be verified.
            if recipient_list:
                recipient = recipient_list
                body = error_message
                mail_subject = subject or "Digitap UAN Batch Processing Engine"
                sender_name = Configuration.SENDER_NAME_FOR_CLIENT

            else:
                sender_name = Configuration.SENDER_NAME
                recipient = Configuration.RECIPIENT_LIST
                body = (f"Hi Team,\r\n "
                        f"An exception has been raised in  environment {ENV} Batch Engine  for "
                        f" Request_id : {txn_id} with message as: \n {error_message} ")
                mail_subject = f"Digitap UAN Batch Processing Engine Issue in - {ENV} Environment on {datetime.now().strftime('%d-%m-%Y')}"

            # Replace smtp_username with your Amazon SES SMTP user name.
            smtp_username = Configuration.SMTP_USERNAME

            # Replace smtp_password with your Amazon SES SMTP password.
            smtp_password = Configuration.SMTP_PASSWORD

            # If you're using Amazon SES in an AWS Region other than US West (Oregon),
            # replace email-smtp.us-west-2.amazonaws.com with the Amazon SES SMTP
            # endpoint in the appropriate region.
            ses_host = Configuration.SMTP_HOST
            ses_port = Configuration.SMTP_PORT

            # The email body for recipients with non-HTML email clients.

            # Create message container - the correct MIME type is multipart/alternative.
            msg = MIMEMultipart('alternative')
            msg['Subject'] = mail_subject
            msg['From'] = email.utils.formataddr((sender_name, sender_email))
            msg['To'] = ", ".join(recipient)

            # Record the MIME types of both parts - text/plain and text/html.
            part1 = MIMEText(body, 'plain')

            # Attach parts into message container.
            msg.attach(part1)

            # Try to send the message.
            server = smtplib.SMTP(ses_host, ses_port)
            server.ehlo()
            server.starttls()
            # smtp lib docs recommend calling ehlo() before & after start tls()
            server.ehlo()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, recipient, msg.as_string())
            server.close()

            logger.info("Mail has been sent through the SES API")

        except Exception:
            logger.exception("Exception happened while sending the exception email to the customer")
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(to_email, subject, body, file_bytes=None, filename=None):
    email_user = os.environ.get("EMAIL_USER")
    email_password = os.environ.get("EMAIL_PASSWORD")
    email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    email_port = int(os.environ.get("EMAIL_PORT", "587"))

    if not email_user or not email_password:
        raise RuntimeError("Email delivery is not configured. Set EMAIL_USER and EMAIL_PASSWORD first.")

    message = MIMEMultipart()
    message["From"] = email_user
    message["To"] = to_email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    if file_bytes and filename:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(file_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={filename}")
        message.attach(part)

    server = smtplib.SMTP(email_host, email_port)
    try:
        server.starttls()
        server.login(email_user, email_password)
        server.send_message(message)
    except smtplib.SMTPAuthenticationError as exc:
        raise RuntimeError(
            "Email login failed. Use the correct EMAIL_USER and a Gmail App Password, not your normal Gmail password."
        ) from exc
    finally:
        server.quit()

import base64
import json
import os
import smtplib
import socket
import urllib.error
import urllib.request
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(to_email, subject, body, file_bytes=None, filename=None):
    provider = os.environ.get("EMAIL_PROVIDER", "smtp").lower()
    if provider == "brevo":
        return _send_brevo_email(to_email, subject, body, file_bytes=file_bytes, filename=filename)

    return _send_smtp_email(to_email, subject, body, file_bytes=file_bytes, filename=filename)


def _send_smtp_email(to_email, subject, body, file_bytes=None, filename=None):
    email_user = os.environ.get("EMAIL_USER")
    email_password = os.environ.get("EMAIL_PASSWORD")
    email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    email_port = int(os.environ.get("EMAIL_PORT", "587"))
    email_timeout = int(os.environ.get("EMAIL_TIMEOUT", "20"))
    use_ssl = os.environ.get("EMAIL_USE_SSL", "").lower() in {"1", "true", "yes"} or email_port == 465

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

    server = None
    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(email_host, email_port, timeout=email_timeout)
        else:
            server = smtplib.SMTP(email_host, email_port, timeout=email_timeout)
            server.starttls()
        server.login(email_user, email_password)
        server.send_message(message)
    except (socket.timeout, TimeoutError, OSError) as exc:
        raise RuntimeError(
            f"Email server connection failed. Check EMAIL_HOST, EMAIL_PORT, and outbound SMTP access: {exc}"
        ) from exc
    except smtplib.SMTPAuthenticationError as exc:
        raise RuntimeError(
            "Email login failed. Use the correct EMAIL_USER and a Gmail App Password, not your normal Gmail password."
        ) from exc
    finally:
        if server:
            server.quit()


def _send_brevo_email(to_email, subject, body, file_bytes=None, filename=None):
    api_key = os.environ.get("BREVO_API_KEY")
    sender_email = os.environ.get("EMAIL_USER")
    sender_name = os.environ.get("EMAIL_SENDER_NAME", "iEnglish")
    timeout = int(os.environ.get("EMAIL_TIMEOUT", "20"))

    if not api_key:
        raise RuntimeError("Brevo delivery is not configured. Set BREVO_API_KEY first.")
    if not sender_email:
        raise RuntimeError("Email delivery is not configured. Set EMAIL_USER first.")

    payload = {
        "sender": {"name": sender_name, "email": sender_email},
        "to": [{"email": to_email}],
        "subject": subject,
        "textContent": body,
    }

    if file_bytes and filename:
        payload["attachment"] = [
            {
                "content": base64.b64encode(bytes(file_bytes)).decode("ascii"),
                "name": filename,
            }
        ]

    request = urllib.request.Request(
        "https://api.brevo.com/v3/smtp/email",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "accept": "application/json",
            "api-key": api_key,
            "content-type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status >= 400:
                raise RuntimeError(f"Brevo email API responded with {response.status}")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Brevo email API failed with {exc.code}: {error_body}") from exc
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
        raise RuntimeError(f"Brevo email API connection failed: {exc}") from exc

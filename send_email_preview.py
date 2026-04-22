import argparse

from flask import render_template

from app import app
from mail_service import send_email


def build_preview_html():
    public_base = (app.config.get("PUBLIC_BASE_URL") or "").rstrip("/")
    logo_url = app.config.get("EMAIL_LOGO_URL")
    if not logo_url and public_base:
        logo_url = f"{public_base}/static/logo.png"
    if not logo_url:
        logo_url = "https://ienglish-crm-taki.com/static/logo.png"

    return render_template(
        "email_report_body.html",
        logo_url=logo_url,
        student_name="Omar Ahmad",
        report_type_label="Midterm",
        course="General English",
        class_type="Group Adult",
        teacher_name="Mr. Hussam",
        view_url=f"{public_base}/view/preview" if public_base else None,
    )


def main():
    parser = argparse.ArgumentParser(description="Send a test email using the report email body template.")
    parser.add_argument("recipient", help="Email address that should receive the preview.")
    args = parser.parse_args()

    with app.app_context():
        html_body = build_preview_html()
        text_body = (
            "Dear Parent/Guardian,\n\n"
            "This is a preview of the iEnglish Institute student progress report email body.\n\n"
            "iEnglish Institute"
        )
        send_email(
            to_email=args.recipient,
            subject="Preview - Student Progress Report",
            body=text_body,
            html_body=html_body,
        )

    print(f"Preview email sent to {args.recipient}")


if __name__ == "__main__":
    main()

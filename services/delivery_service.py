import os
from datetime import datetime

from flask import current_app, render_template

import models
from mail_service import send_email

from . import ServiceError
from . import report_service
from .user_service import user_has_role


DELIVERY_TARGETS = {
    "student_email": {"channel": "email"},
}


def send(report_id, channel, actor, recipient=None, delivery_target=None):
    if not user_has_role(actor, "operation", "admin", "manager"):
        raise ServiceError("Only operation can send reports", 403)
    if channel != "email":
        raise ServiceError("Only email delivery is supported", 400)
    if delivery_target not in DELIVERY_TARGETS:
        raise ServiceError("Unsupported delivery target", 400)
    if DELIVERY_TARGETS[delivery_target]["channel"] != channel:
        raise ServiceError("Delivery target does not match the selected channel", 400)

    report = report_service.get_report_for_user(report_id, actor, include_json=True)
    if report["status"] not in {"approved", "delivered"}:
        raise ServiceError("Only approved or delivered reports can be sent", 400)
    recipients = _normalize_recipients(
        recipient or _default_recipient(report, channel),
        channel=channel,
    )
    if not recipients:
        raise ServiceError("Recipient is required", 400)

    delivery_mode = "live"
    errors = []
    delivered_to = []
    for single_recipient in recipients:
        log_id = models.create_delivery_log(
            report_id=report_id,
            channel=channel,
            delivery_target=delivery_target,
            recipient=single_recipient,
            status="pending",
        )

        try:
            _send_email_delivery(report, single_recipient)
        except Exception as exc:
            models.update_delivery_log(log_id, "failed", str(exc), None)
            errors.append(f"{single_recipient}: {exc}")
            continue

        sent_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        models.update_delivery_log(log_id, "sent", None, sent_at)
        delivered_to.append(single_recipient)

    if not delivered_to:
        raise ServiceError(f"Delivery failed: {' | '.join(errors)}", 502)

    updated_report = models.get_report_detail(report_id, include_json=True)
    if updated_report["status"] == "approved":
        updated_report = report_service.mark_report_delivered(
            report_id,
            actor,
            f"Delivered via {channel} to {', '.join(delivered_to)}",
        )

    return {
        "report": updated_report,
        "delivery_mode": delivery_mode,
        "delivered_to": delivered_to,
    }


def _send_email_delivery(report, recipient):
    student_name = report["student_name"]
    pdf_url = report.get("pdf_path") or report_service._build_public_pdf_url(report["id"])
    pdf_bytes = None
    filename = f"report_{report['id']}.pdf"

    try:
        report_service.refresh_report_pdf(report["id"])
        pdf_payload = report_service.get_public_report_pdf(report["id"])
        document = pdf_payload["document"]
        pdf_bytes = document.get("content")
        filename = os.path.basename(document.get("storage_key") or filename) or filename
    except Exception as exc:
        raise RuntimeError(f"PDF attachment is not available: {exc}") from exc

    if not pdf_bytes:
        raise RuntimeError("PDF attachment is not available")

    text_body = _build_text_email_body(report, pdf_url)
    html_body = _build_html_email_body(report, pdf_url)

    send_email(
        to_email=recipient,
        subject="Student Progress Report",
        body=text_body,
        file_bytes=pdf_bytes,
        filename=filename,
        html_body=html_body,
    )


def _build_text_email_body(report, pdf_url=None):
    student_name = report.get("student_name") or "the student"
    return (
        f"Dear Parent/Guardian,\n\n"
        f"We are pleased to share the latest academic progress report for {student_name}.\n"
        f"The full report is attached as a PDF for your review and records.\n\n"
        f"View report: {pdf_url or report_service._build_public_pdf_url(report['id'])}\n\n"
        f"iEnglish Institute"
    )


def _build_html_email_body(report, pdf_url=None):
    report_json = report.get("report_json") or {}
    student = report_json.get("student") or {}
    public_base = (current_app.config.get("PUBLIC_BASE_URL") or "").rstrip("/")
    logo_url = os.environ.get("EMAIL_LOGO_URL")
    if not logo_url and public_base:
        logo_url = f"{public_base}/static/logo.png"
    if not logo_url:
        logo_url = "https://ienglish-crm-taki.com/static/logo.png"

    report_type = report.get("report_type") or report_json.get("report_type") or ""
    report_type_label = "Final" if report_type == "final" else "Midterm"
    return render_template(
        "email_report_body.html",
        logo_url=logo_url,
        student_name=report.get("student_name") or student.get("name") or "Student",
        report_type_label=report_type_label,
        course=report.get("course") or student.get("course") or "-",
        class_type=student.get("class_type") or "-",
        teacher_name=report.get("teacher_name") or student.get("teacher") or "-",
        view_url=pdf_url or report_service._build_public_pdf_url(report["id"]),
    )


def _default_recipient(report, channel):
    return report.get("student_email")


def _normalize_recipients(recipient, channel=None):
    if recipient is None:
        return []
    if isinstance(recipient, list):
        raw_values = recipient
    else:
        raw_values = str(recipient).replace(";", "\n").replace(",", "\n").splitlines()

    recipients = []
    for value in raw_values:
        normalized = str(value).strip()
        if normalized and normalized not in recipients:
            recipients.append(normalized)
    return recipients

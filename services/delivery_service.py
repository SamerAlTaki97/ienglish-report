import json
import urllib.request
from datetime import datetime

import models
from mail_service import send_email

from . import ServiceError
from . import report_service
from .user_service import user_has_role


DELIVERY_TARGETS = {
    "student_email": {"channel": "email"},
    "student_whatsapp": {"channel": "whatsapp"},
    "sales_whatsapp": {"channel": "whatsapp"},
}


def send(report_id, channel, actor, recipient=None, delivery_target=None):
    if not user_has_role(actor, "operation", "admin"):
        raise ServiceError("Only operation can send reports", 403)
    if channel not in {"email", "whatsapp"}:
        raise ServiceError("Unsupported delivery channel", 400)
    if delivery_target not in DELIVERY_TARGETS:
        raise ServiceError("Unsupported delivery target", 400)
    if DELIVERY_TARGETS[delivery_target]["channel"] != channel:
        raise ServiceError("Delivery target does not match the selected channel", 400)

    report = report_service.get_report_for_user(report_id, actor, include_json=True)
    if report["status"] not in {"approved", "delivered"}:
        raise ServiceError("Only approved or delivered reports can be sent", 400)
    if channel != "email" and not report.get("pdf_path"):
        raise ServiceError("Report PDF is not available", 400)
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
            if channel == "email":
                _send_email_delivery(report, single_recipient)
            else:
                outcome = _send_whatsapp_delivery(report, single_recipient)
                if outcome == "dry_run":
                    delivery_mode = "dry_run"
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
    send_email(
        to_email=recipient,
        subject="Student Progress Report",
        body=(
            f"Hello,\n\n"
            f"Your academic report for {student_name} is ready.\n"
            f"You can view or download it here:\n{pdf_url}\n\n"
            f"Regards."
        ),
        file_bytes=None,
        filename=None,
    )


def _send_whatsapp_delivery(report, recipient):
    api_url = report_service.current_app.config.get("WHATSAPP_API_URL")
    token = report_service.current_app.config.get("WHATSAPP_API_TOKEN")
    dry_run = report_service.current_app.config.get("WHATSAPP_DRY_RUN", True)

    payload = {
        "to": recipient,
        "type": "document",
        "document": {
            "link": report["pdf_path"],
            "filename": f"report_{report['id']}.pdf",
        },
        "metadata": {
            "report_id": report["id"],
            "student_phone": report["student_id"],
        },
    }

    if dry_run or not api_url:
        return "dry_run"

    request = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        if response.status >= 400:
            raise RuntimeError(f"WhatsApp API responded with {response.status}")
    return "live"


def _default_recipient(report, channel):
    if channel == "email":
        return report.get("student_email")
    return report["student_id"]


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
        if channel == "whatsapp":
            normalized = _normalize_whatsapp_number(normalized)
        if normalized and normalized not in recipients:
            recipients.append(normalized)
    return recipients


def _normalize_whatsapp_number(number):
    cleaned = str(number or "").strip()
    if not cleaned:
        return ""

    cleaned = cleaned.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")

    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]

    if cleaned.startswith("+"):
        digits = "+" + "".join(ch for ch in cleaned[1:] if ch.isdigit())
    else:
        digits_only = "".join(ch for ch in cleaned if ch.isdigit())
        if digits_only.startswith("971"):
            digits = "+" + digits_only
        elif digits_only.startswith("05") and len(digits_only) == 10:
            digits = "+971" + digits_only[1:]
        elif digits_only.startswith("5") and len(digits_only) == 9:
            digits = "+971" + digits_only
        else:
            raise ServiceError(
                f"WhatsApp number '{number}' must be in international format like +9715XXXXXXXX or a valid UAE mobile number",
                400,
            )

    if not digits.startswith("+") or len(digits) < 11:
        raise ServiceError(
            f"WhatsApp number '{number}' is invalid",
            400,
        )

    return digits

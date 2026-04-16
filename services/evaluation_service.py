import models

from . import ServiceError
from .user_service import user_has_role


def create_evaluation(report_id, data, actor):
    if not user_has_role(actor, "operation", "admin"):
        raise ServiceError("Only operation can create evaluations", 403)

    report = models.get_report_detail(report_id, include_json=False)
    if not report:
        raise ServiceError("Report not found", 404)
    if report["status"] != "delivered":
        raise ServiceError("Evaluations are only allowed after delivery", 400)
    if models.get_evaluation_by_report(report_id):
        raise ServiceError("Only one evaluation is allowed per report", 409)
    if not report.get("sales_id"):
        raise ServiceError("Student is not assigned to a sales user", 400)

    evaluation_id = models.create_evaluation(
        report_id=report_id,
        teacher_id=report["teacher_id"],
        sales_id=report["sales_id"],
        rating_teacher=data.get("rating_teacher"),
        rating_sales=data.get("rating_sales"),
        teacher_notes=data.get("teacher_notes"),
        sales_notes=data.get("sales_notes"),
        created_by=actor["id"],
    )

    evaluation = models.get_evaluation_by_report(report_id)
    evaluation["id"] = evaluation_id
    return evaluation


def get_evaluation(report_id, actor):
    if not user_has_role(actor, "operation", "admin"):
        raise ServiceError("Only operation can view evaluations", 403)
    evaluation = models.get_evaluation_by_report(report_id)
    if not evaluation:
        raise ServiceError("Evaluation not found", 404)
    return evaluation

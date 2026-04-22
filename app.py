import io
from functools import wraps

from flask import Flask, Response, jsonify, redirect, render_template, request, send_file, session, url_for

from config import AppConfig
from database import init_db
from services import ServiceError
from services import delivery_service, report_service, user_service

app = Flask(__name__)
app.config.from_object(AppConfig)
app.secret_key = app.config["SECRET_KEY"]

init_db()

DISPLAY_MR_NAMES = {
    "samer",
    "haider",
    "hussam",
    "fouad",
    "mazen",
    "zain",
    "amin",
    "karam",
    "omar",
}


@app.errorhandler(ServiceError)
def handle_service_error(error):
    return jsonify({"error": error.message}), error.status_code


def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        user = current_user()
        if not user:
            if request.accept_mimetypes.accept_json or request.path.startswith(("/api/", "/reports/", "/generate")):
                return jsonify({"error": "Your session expired. Please log in again."}), 401
            return redirect(url_for("login"))
        if user.get("must_change_password") and request.endpoint not in {"change_password", "logout"}:
            if request.accept_mimetypes.accept_json or request.path.startswith(("/api/", "/reports/", "/generate")):
                return jsonify({"error": "Password change is required before continuing."}), 403
            return redirect(url_for("change_password"))
        return view(*args, **kwargs)

    return wrapper


def role_required(*roles):
    def decorator(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                if request.accept_mimetypes.accept_json or request.path.startswith(("/api/", "/reports/", "/generate")):
                    return jsonify({"error": "Your session expired. Please log in again."}), 401
                return redirect(url_for("login"))
            if user.get("must_change_password") and request.endpoint not in {"change_password", "logout"}:
                if request.accept_mimetypes.accept_json or request.path.startswith(("/api/", "/reports/", "/generate")):
                    return jsonify({"error": "Password change is required before continuing."}), 403
                return redirect(url_for("change_password"))
            if user["role"] != "superadmin" and user["role"] not in roles:
                if request.accept_mimetypes.accept_json or request.path.startswith(("/api/", "/reports/", "/generate")):
                    return jsonify({"error": "Forbidden"}), 403
                return "Forbidden", 403
            return view(*args, **kwargs)

        return wrapper

    return decorator


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    user = user_service.get_user(user_id)
    if not user:
        session.clear()
    return user


def render_with_user(template_name, **context):
    user = current_user()
    raw_name = (user or {}).get("name")
    return render_template(
        template_name,
        user_name=raw_name,
        display_user_name=format_display_name(raw_name),
        user_id=(user or {}).get("id"),
        role=(user or {}).get("role"),
        user_branch=(user or {}).get("branch"),
        **context,
    )


def format_display_name(name):
    clean_name = str(name or "").strip()
    if not clean_name:
        return ""
    if clean_name.lower().startswith(("mr. ", "ms. ")):
        return clean_name
    first_name = clean_name.split()[0].lower()
    prefix = "Mr." if first_name in DISPLAY_MR_NAMES else "Ms."
    return f"{prefix} {clean_name}"


def redirect_for_role(user):
    if user.get("must_change_password"):
        return redirect(url_for("change_password"))
    if user["role"] == "superadmin":
        return redirect(url_for("admin_teachers_page"))
    if user["role"] == "manager":
        return redirect(url_for("admin"))
    if user["role"] == "admin":
        return redirect(url_for("admin"))
    if user["role"] == "operation":
        return redirect(url_for("operation"))
    if user["role"] == "sales":
        return redirect(url_for("sales"))
    return redirect(url_for("teacher"))


@app.route("/")
def home():
    return redirect(url_for("login"))


@app.route("/teacher")
@login_required
@role_required("teacher", "manager")
def teacher():
    user = current_user()
    return render_with_user("form.html")


@app.route("/report")
@login_required
@role_required("teacher", "manager")
def report():
    return render_with_user("report.html")


@app.route("/operation")
@login_required
@role_required("operation", "manager")
def operation():
    return render_with_user("operation.html")


@app.route("/admin")
@login_required
@role_required("admin", "manager")
def admin():
    return render_with_user("Admin.html")


@app.route("/sales")
@login_required
@role_required("sales")
def sales():
    return render_with_user("sales.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        session.clear()
        return render_template("login.html")

    user = user_service.authenticate_user(
        (
            request.form.get("login_user_id")
            or request.form.get("username", "")
        ).strip(),
        request.form.get("login_access_key") or request.form.get("password", ""),
    )
    if not user:
        return render_template("login.html", error="Invalid credentials")

    session["user_id"] = user["id"]

    return redirect_for_role(user)


@app.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    user = current_user()
    if request.method == "GET":
        return render_template("change_password.html", user_name=user.get("name"))

    try:
        user_service.change_own_password(
            user,
            request.form.get("new_password", ""),
            request.form.get("confirm_password", ""),
        )
    except ServiceError as error:
        return render_template("change_password.html", user_name=user.get("name"), error=error.message), error.status_code

    session["user_id"] = user["id"]
    updated_user = user_service.get_user(user["id"])
    return redirect_for_role(updated_user)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/api/users")
@login_required
def users_lookup():
    role = request.args.get("role")
    branch = request.args.get("branch")
    return jsonify(user_service.list_visible_users(current_user(), role=role, branch=branch))


@app.route("/students/search")
@login_required
def students_search():
    phone = request.args.get("phone", "").strip()
    if not phone:
        return jsonify([])
    return jsonify(report_service.search_students(phone, current_user()))


@app.route("/admin/reports")
@login_required
@role_required("admin", "manager")
def admin_reports():
    reports = report_service.list_reports_for_user(
        current_user(),
        phone=request.args.get("phone"),
        status=request.args.get("status"),
        teacher_id=request.args.get("teacher_id"),
        branch=request.args.get("branch"),
    )
    return jsonify(reports)


@app.route("/operation/reports")
@login_required
@role_required("operation", "manager")
def operation_reports():
    reports = report_service.list_reports_for_user(
        current_user(),
        phone=request.args.get("phone"),
        status=request.args.get("status"),
        teacher_id=request.args.get("teacher_id"),
        branch=request.args.get("branch"),
    )
    return jsonify(reports)


@app.route("/sales/reports")
@login_required
@role_required("sales")
def sales_reports():
    reports = report_service.list_reports_for_user(
        current_user(),
        phone=request.args.get("phone"),
        status=request.args.get("status"),
    )
    return jsonify(reports)


@app.route("/reports/<int:report_id>")
@login_required
def report_detail(report_id):
    return jsonify(report_service.get_report_for_user(report_id, current_user(), include_json=True))


@app.route("/generate", methods=["POST"])
@login_required
@role_required("teacher", "manager")
def generate():
    try:
        report = report_service.generate_report(request.get_json() or {}, current_user())
    except ServiceError:
        raise
    except Exception as error:
        app.logger.exception("Unexpected error while generating report")
        return jsonify({"error": f"Report generation failed: {error}"}), 500

    payload = dict(report["report_json"])
    payload["report_id"] = report["id"]
    payload["status"] = report["status"]
    payload["pdf_url"] = report["pdf_path"]
    return jsonify(payload)


@app.route("/reports/<int:report_id>/edit", methods=["PUT"])
@login_required
@role_required("teacher", "manager")
def edit_report(report_id):
    report = report_service.update_report(report_id, request.get_json() or {}, current_user())
    payload = dict(report["report_json"])
    payload["report_id"] = report["id"]
    payload["status"] = report["status"]
    payload["pdf_url"] = report["pdf_path"]
    return jsonify(payload)


@app.route("/reports/<int:report_id>/submit", methods=["POST"])
@login_required
@role_required("teacher", "manager")
def submit_report(report_id):
    report = report_service.submit_report(report_id, current_user())
    return jsonify(
        {
            "status": report["status"],
            "report_id": report["id"],
        }
    )


@app.route("/reports/<int:report_id>/approve", methods=["POST"])
@login_required
@role_required("operation", "manager")
def approve_report(report_id):
    report = report_service.approve_report(report_id, current_user())
    return jsonify({"status": report["status"], "report_id": report["id"]})


@app.route("/reports/<int:report_id>/contact", methods=["PATCH"])
@login_required
@role_required("operation", "manager")
def update_report_contact(report_id):
    data = request.get_json() or {}
    report = report_service.update_student_contact(
        report_id,
        current_user(),
        phone=data.get("student_phone"),
        email=data.get("student_email"),
        sales_id=data.get("sales_id"),
    )
    return jsonify(
        {
            "report_id": report["id"],
            "student_id": report["student_id"],
            "student_email": report.get("student_email"),
            "sales_id": report.get("sales_id"),
            "sales_name": report.get("sales_name"),
        }
    )


@app.route("/reports/<int:report_id>/deliver", methods=["POST"])
@login_required
@role_required("operation", "manager")
def deliver_report(report_id):
    data = request.get_json() or {}
    if "student_phone" in data or "student_email" in data:
        report_service.update_student_contact(
            report_id,
            current_user(),
            phone=data.get("student_phone"),
            email=data.get("student_email"),
            refresh_pdf=False,
        )
    result = delivery_service.send(
        report_id=report_id,
        channel="email",
        actor=current_user(),
        recipient=data.get("recipient"),
        delivery_target="student_email",
    )
    return jsonify(
        {
            "status": result["report"]["status"],
            "report_id": result["report"]["id"],
            "delivery_mode": result["delivery_mode"],
            "delivered_to": result["delivered_to"],
        }
    )


@app.route("/send/<int:report_id>", methods=["POST"])
@login_required
@role_required("operation", "manager")
def send_report(report_id):
    data = request.get_json() or {}
    if "student_phone" in data or "student_email" in data:
        report_service.update_student_contact(
            report_id,
            current_user(),
            phone=data.get("student_phone"),
            email=data.get("student_email"),
            refresh_pdf=False,
        )
    result = delivery_service.send(
        report_id=report_id,
        channel="email",
        actor=current_user(),
        recipient=data.get("email") or data.get("recipient"),
        delivery_target="student_email",
    )
    return jsonify(
        {
            "status": result["report"]["status"],
            "report_id": result["report"]["id"],
            "delivery_mode": result["delivery_mode"],
            "delivered_to": result["delivered_to"],
        }
    )


@app.route("/view/<int:report_id>")
@login_required
def view_pdf(report_id):
    report = report_service.get_report_for_user(report_id, current_user(), include_json=True)
    html = report_service.render_report_html(
        report["report_json"],
        preview_mode=True,
        report_meta={
            "id": report["id"],
            "status": report["status"],
            "pdf_download_url": url_for("public_report_pdf", report_id=report_id),
        },
    )
    return html


@app.route("/storage/reports/<int:report_id>/pdf")
def public_report_pdf(report_id):
    report_service.refresh_report_pdf(report_id)
    payload = report_service.get_public_report_pdf(report_id)
    report = payload["report"]
    document = payload["document"]
    as_download = request.args.get("download") == "1"

    if document["storage_provider"] == "s3" and report.get("pdf_path"):
        return redirect(report["pdf_path"])

    return Response(
        document["content"],
        mimetype=document.get("content_type", "application/pdf"),
        headers={
            "Content-Disposition": f'{"attachment" if as_download else "inline"}; filename="report_{report_id}.pdf"',
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.route("/admin/teachers")
@login_required
@role_required("superadmin")
def admin_teachers():
    return jsonify(
        user_service.list_visible_users(
            current_user(),
            role=request.args.get("role"),
            branch=request.args.get("branch"),
        )
    )


@app.route("/admin_teachers")
@login_required
@role_required("superadmin")
def admin_teachers_page():
    return render_with_user("admin_teachers.html")


@app.route("/admin/teachers/add", methods=["POST"])
@login_required
@role_required("superadmin")
def admin_add_teacher():
    data = request.get_json() or {}
    user = user_service.create_user_account(
        current_user(),
        name=data.get("name", "").strip(),
        username=data.get("username", "").strip(),
        password=data.get("password", ""),
        role=data.get("role", "teacher"),
        branch=data.get("branch"),
    )
    return jsonify(user)


@app.route("/admin/users/<int:user_id>/reset-password", methods=["POST"])
@login_required
@role_required("superadmin")
def admin_reset_password(user_id):
    data = request.get_json() or {}
    return jsonify(
        user_service.reset_user_password(
            current_user(),
            data.get("account_password", ""),
            user_id,
            data.get("new_password", ""),
        )
    )


@app.route("/admin/users/<int:user_id>/branch", methods=["PATCH"])
@login_required
@role_required("superadmin")
def admin_update_user_branch(user_id):
    data = request.get_json() or {}
    return jsonify(user_service.update_user_branch(current_user(), user_id, data.get("branch")))


@app.route("/admin/users/<int:user_id>", methods=["DELETE"])
@login_required
@role_required("superadmin")
def admin_delete_user(user_id):
    data = request.get_json() or {}
    return jsonify(
        user_service.delete_user_account(
            current_user(),
            data.get("account_password", ""),
            user_id,
        )
    )


@app.route("/word", methods=["POST"])
@login_required
def word():
    data = request.get_json() or {}
    student = data.get("student", {})
    docx_bytes = report_service.generate_word_document(data)
    return send_file(
        io.BytesIO(docx_bytes),
        as_attachment=True,
        download_name=f"{student.get('name', 'Student')}.docx",
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )


@app.route("/chart", methods=["POST"])
@login_required
def chart():
    chart_bytes = report_service.generate_chart(request.get_json() or {})
    return send_file(io.BytesIO(chart_bytes), mimetype="image/png")


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)

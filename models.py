import json

from database import get_connection


def create_user(
    name,
    username,
    password_hash,
    role,
    branch=None,
    is_active=1,
    password_plain=None,
    email=None,
    must_change_password=1,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO users (name, username, email, password_hash, password_plain, role, branch, must_change_password, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            username,
            email,
            password_hash,
            password_plain,
            role,
            branch,
            must_change_password,
            is_active,
        ),
    )
    conn.commit()
    user_id = cursor.lastrowid
    conn.close()
    return user_id


def get_user_by_email(email):
    conn = get_connection()
    row = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_username(username):
    conn = get_connection()
    row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_username_ci(username):
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM users WHERE lower(username)=lower(?)",
        (username,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_user_by_id(user_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_users(role=None, branch=None, active_only=True, exclude_roles=None):
    conn = get_connection()
    query = "SELECT * FROM users WHERE 1=1"
    params = []

    if role:
        query += " AND role=?"
        params.append(role)
    if branch:
        query += " AND branch=?"
        params.append(branch)
    if active_only:
        query += " AND is_active=1"
    if exclude_roles:
        placeholders = ",".join("?" for _ in exclude_roles)
        query += f" AND role NOT IN ({placeholders})"
        params.extend(exclude_roles)

    query += " ORDER BY name"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def deactivate_user(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET is_active=0 WHERE id=?", (user_id,))
    conn.commit()
    changed = cursor.rowcount
    conn.close()
    return changed > 0


def reset_user_password(user_id, password_hash):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE users
        SET password_hash=?,
            password_plain=NULL,
            must_change_password=1
        WHERE id=?
        """,
        (password_hash, user_id),
    )
    conn.commit()
    changed = cursor.rowcount
    conn.close()
    return changed > 0


def change_user_password(user_id, password_hash):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE users
        SET password_hash=?,
            password_plain=NULL,
            must_change_password=0
        WHERE id=?
        """,
        (password_hash, user_id),
    )
    conn.commit()
    changed = cursor.rowcount
    conn.close()
    return changed > 0


def update_user_branch(user_id, branch):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET branch=? WHERE id=?", (branch, user_id))
    conn.commit()
    changed = cursor.rowcount
    conn.close()
    return changed > 0


def upsert_student(phone, name, branch=None, email=None, sales_id=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO students (phone, name, branch, email, sales_id)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(phone) DO UPDATE SET
            name=excluded.name,
            branch=COALESCE(excluded.branch, students.branch),
            email=COALESCE(excluded.email, students.email),
            sales_id=COALESCE(excluded.sales_id, students.sales_id)
        """,
        (phone, name, branch, email, sales_id),
    )
    conn.commit()
    conn.close()
    return get_student_by_phone(phone)


def get_student_by_phone(phone):
    conn = get_connection()
    row = conn.execute("SELECT * FROM students WHERE phone=?", (phone,)).fetchone()
    conn.close()
    return dict(row) if row else None


def search_students_by_phone(phone_query):
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT s.*, sales.name AS sales_name
        FROM students s
        LEFT JOIN users sales ON sales.id = s.sales_id
        WHERE s.phone LIKE ?
        ORDER BY s.name
        """,
        (f"%{phone_query}%",),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def create_report(
    student_id,
    teacher_id,
    report_type,
    level,
    course,
    report_json,
    status="draft",
    created_by=None,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO reports (
            student_id,
            teacher_id,
            report_type,
            level,
            course,
            report_json,
            status,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            student_id,
            teacher_id,
            report_type,
            level,
            course,
            json.dumps(report_json, ensure_ascii=False),
            status,
            created_by,
        ),
    )
    conn.commit()
    report_id = cursor.lastrowid
    conn.close()
    return report_id


def update_report_content(report_id, report_type=None, level=None, course=None, report_json=None):
    conn = get_connection()
    current = conn.execute("SELECT * FROM reports WHERE id=?", (report_id,)).fetchone()
    if not current:
        conn.close()
        return False

    current = dict(current)
    conn.execute(
        """
        UPDATE reports
        SET report_type=?,
            level=?,
            course=?,
            report_json=?
        WHERE id=?
        """,
        (
            report_type or current["report_type"],
            level if level is not None else current["level"],
            course if course is not None else current["course"],
            json.dumps(report_json, ensure_ascii=False)
            if report_json is not None
            else current["report_json"],
            report_id,
        ),
    )
    conn.commit()
    conn.close()
    return True


def update_report_status(report_id, status):
    conn = get_connection()
    conn.execute("UPDATE reports SET status=? WHERE id=?", (status, report_id))
    conn.commit()
    conn.close()


def update_report_pdf_url(report_id, pdf_url):
    conn = get_connection()
    conn.execute("UPDATE reports SET pdf_path=? WHERE id=?", (pdf_url, report_id))
    conn.commit()
    conn.close()


def delete_report(report_id):
    conn = get_connection()
    cursor = conn.cursor()
    report = cursor.execute(
        "SELECT student_id FROM reports WHERE id=?",
        (report_id,),
    ).fetchone()
    if not report:
        conn.close()
        return False

    student_id = report["student_id"]
    cursor.execute("DELETE FROM reports WHERE id=?", (report_id,))
    if student_id:
        still_used = cursor.execute(
            "SELECT 1 FROM reports WHERE student_id=? LIMIT 1",
            (student_id,),
        ).fetchone()
        if not still_used:
            cursor.execute("DELETE FROM students WHERE phone=?", (student_id,))

    _reset_autoincrement(cursor, "reports")
    _reset_autoincrement(cursor, "report_tracking")
    _reset_autoincrement(cursor, "delivery_logs")
    _reset_autoincrement(cursor, "evaluations")
    conn.commit()
    conn.close()
    return True


def get_report(report_id, include_json=False):
    conn = get_connection()
    row = conn.execute("SELECT * FROM reports WHERE id=?", (report_id,)).fetchone()
    conn.close()
    if not row:
        return None

    report = dict(row)
    if include_json:
        report["report_json"] = _deserialize_json(report.get("report_json"))
    return report


def get_report_detail(report_id, include_json=True):
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            r.*,
            CASE
                WHEN r.student_id LIKE 'legacy-%' THEN COALESCE(json_extract(r.report_json, '$.student.phone'), r.student_id)
                ELSE COALESCE(r.student_id, json_extract(r.report_json, '$.student.phone'))
            END AS display_student_phone,
            COALESCE(s.name, json_extract(r.report_json, '$.student.name')) AS student_name,
            COALESCE(s.branch, json_extract(r.report_json, '$.student.branch')) AS student_branch,
            COALESCE(s.email, json_extract(r.report_json, '$.student.email')) AS student_email,
            CASE
                WHEN COALESCE(json_extract(r.report_json, '$.student.sales_mode'), '') = 'direct' THEN NULL
                ELSE COALESCE(s.sales_id, CAST(json_extract(r.report_json, '$.student.sales_id') AS INTEGER))
            END AS sales_id,
            COALESCE(json_extract(r.report_json, '$.student.sales_mode'), '') AS sales_mode,
            teacher.name AS teacher_name,
            teacher.branch AS teacher_branch,
            CASE
                WHEN COALESCE(json_extract(r.report_json, '$.student.sales_mode'), '') = 'direct' THEN 'Direct'
                ELSE COALESCE(sales.name, json_extract(r.report_json, '$.student.sales_name'))
            END AS sales_name,
            creator.name AS created_by_name,
            (
                SELECT dl.sent_at
                FROM delivery_logs dl
                WHERE dl.report_id = r.id AND dl.status = 'sent'
                ORDER BY dl.sent_at DESC, dl.id DESC
                LIMIT 1
            ) AS sent_at,
            (
                SELECT dl.channel
                FROM delivery_logs dl
                WHERE dl.report_id = r.id AND dl.status = 'sent'
                ORDER BY dl.sent_at DESC, dl.id DESC
                LIMIT 1
            ) AS last_delivery_channel,
            EXISTS(
                SELECT 1 FROM delivery_logs dl
                WHERE dl.report_id = r.id AND dl.status = 'sent' AND dl.delivery_target = 'student_email'
            ) AS sent_student_email
        FROM reports r
        LEFT JOIN students s ON s.phone = r.student_id
        JOIN users teacher ON teacher.id = r.teacher_id
        LEFT JOIN users sales ON sales.id = COALESCE(s.sales_id, CAST(json_extract(r.report_json, '$.student.sales_id') AS INTEGER))
        LEFT JOIN users creator ON creator.id = r.created_by
        WHERE r.id=?
        """,
        (report_id,),
    ).fetchone()
    conn.close()

    if not row:
        return None

    report = dict(row)
    if include_json:
        report["report_json"] = _deserialize_json(report.get("report_json"))
    return report


def list_reports(phone=None, status=None, teacher_id=None, sales_id=None, created_by=None, branch=None):
    conn = get_connection()
    query = """
        SELECT
            r.*,
            CASE
                WHEN r.student_id LIKE 'legacy-%' THEN COALESCE(json_extract(r.report_json, '$.student.phone'), r.student_id)
                ELSE COALESCE(r.student_id, json_extract(r.report_json, '$.student.phone'))
            END AS display_student_phone,
            COALESCE(s.name, json_extract(r.report_json, '$.student.name')) AS student_name,
            COALESCE(s.branch, json_extract(r.report_json, '$.student.branch')) AS student_branch,
            COALESCE(s.email, json_extract(r.report_json, '$.student.email')) AS student_email,
            CASE
                WHEN COALESCE(json_extract(r.report_json, '$.student.sales_mode'), '') = 'direct' THEN NULL
                ELSE COALESCE(s.sales_id, CAST(json_extract(r.report_json, '$.student.sales_id') AS INTEGER))
            END AS sales_id,
            COALESCE(json_extract(r.report_json, '$.student.sales_mode'), '') AS sales_mode,
            teacher.name AS teacher_name,
            teacher.branch AS teacher_branch,
            CASE
                WHEN COALESCE(json_extract(r.report_json, '$.student.sales_mode'), '') = 'direct' THEN 'Direct'
                ELSE COALESCE(sales.name, json_extract(r.report_json, '$.student.sales_name'))
            END AS sales_name,
            creator.name AS created_by_name,
            (
                SELECT dl.sent_at
                FROM delivery_logs dl
                WHERE dl.report_id = r.id AND dl.status = 'sent'
                ORDER BY dl.sent_at DESC, dl.id DESC
                LIMIT 1
            ) AS sent_at,
            (
                SELECT dl.channel
                FROM delivery_logs dl
                WHERE dl.report_id = r.id AND dl.status = 'sent'
                ORDER BY dl.sent_at DESC, dl.id DESC
                LIMIT 1
            ) AS last_delivery_channel,
            EXISTS(
                SELECT 1 FROM delivery_logs dl
                WHERE dl.report_id = r.id AND dl.status = 'sent' AND dl.delivery_target = 'student_email'
            ) AS sent_student_email
        FROM reports r
        LEFT JOIN students s ON s.phone = r.student_id
        JOIN users teacher ON teacher.id = r.teacher_id
        LEFT JOIN users sales ON sales.id = COALESCE(s.sales_id, CAST(json_extract(r.report_json, '$.student.sales_id') AS INTEGER))
        LEFT JOIN users creator ON creator.id = r.created_by
        WHERE 1=1
    """
    params = []

    if phone:
        query += " AND CASE WHEN r.student_id LIKE 'legacy-%' THEN COALESCE(json_extract(r.report_json, '$.student.phone'), r.student_id) ELSE COALESCE(r.student_id, json_extract(r.report_json, '$.student.phone')) END LIKE ?"
        params.append(f"%{phone}%")
    if status:
        query += " AND r.status=?"
        params.append(status)
    if teacher_id:
        query += " AND r.teacher_id=?"
        params.append(teacher_id)
    if sales_id:
        query += """
            AND CASE
                WHEN COALESCE(json_extract(r.report_json, '$.student.sales_mode'), '') = 'direct' THEN NULL
                ELSE COALESCE(s.sales_id, CAST(json_extract(r.report_json, '$.student.sales_id') AS INTEGER))
            END=?
        """
        params.append(sales_id)
    if created_by:
        query += " AND r.created_by=?"
        params.append(created_by)
    if branch:
        query += " AND COALESCE(s.branch, json_extract(r.report_json, '$.student.branch'), teacher.branch)=?"
        params.append(branch)

    query += " ORDER BY r.created_at DESC, r.id DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_report_tracking(report_id, status, changed_by=None, notes=None, created_at=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO report_tracking (report_id, status, changed_by, notes, created_at)
        VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
        """,
        (report_id, status, changed_by, notes, created_at),
    )
    conn.commit()
    tracking_id = cursor.lastrowid
    conn.close()
    return tracking_id


def list_report_tracking(report_id):
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT rt.*, u.name AS changed_by_name
        FROM report_tracking rt
        LEFT JOIN users u ON u.id = rt.changed_by
        WHERE rt.report_id=?
        ORDER BY rt.created_at ASC, rt.id ASC
        """,
        (report_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def create_evaluation(
    report_id,
    teacher_id,
    sales_id,
    rating_teacher,
    rating_sales,
    teacher_notes,
    sales_notes,
    created_by,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO evaluations (
            report_id,
            teacher_id,
            sales_id,
            rating_teacher,
            rating_sales,
            teacher_notes,
            sales_notes,
            created_by
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            report_id,
            teacher_id,
            sales_id,
            rating_teacher,
            rating_sales,
            teacher_notes,
            sales_notes,
            created_by,
        ),
    )
    conn.commit()
    evaluation_id = cursor.lastrowid
    conn.close()
    return evaluation_id


def get_evaluation_by_report(report_id):
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            e.*,
            teacher.name AS teacher_name,
            sales.name AS sales_name,
            creator.name AS created_by_name
        FROM evaluations e
        JOIN users teacher ON teacher.id = e.teacher_id
        JOIN users sales ON sales.id = e.sales_id
        LEFT JOIN users creator ON creator.id = e.created_by
        WHERE e.report_id=?
        """,
        (report_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def create_delivery_log(
    report_id,
    channel,
    recipient,
    delivery_target=None,
    status="pending",
    error_message=None,
    sent_at=None,
):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO delivery_logs (report_id, channel, delivery_target, recipient, status, error_message, sent_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (report_id, channel, delivery_target, recipient, status, error_message, sent_at),
    )
    conn.commit()
    log_id = cursor.lastrowid
    conn.close()
    return log_id


def update_delivery_log(log_id, status, error_message=None, sent_at=None):
    conn = get_connection()
    conn.execute(
        """
        UPDATE delivery_logs
        SET status=?,
            error_message=?,
            sent_at=?
        WHERE id=?
        """,
        (status, error_message, sent_at, log_id),
    )
    conn.commit()
    conn.close()


def update_student_contact_for_report(report_id, phone=None, email=None, sales_id=None, sales_id_provided=False):
    conn = get_connection()
    cursor = conn.cursor()

    report_row = cursor.execute(
        """
        SELECT
            r.student_id,
            r.report_json,
            s.email,
            s.name,
            s.branch,
            s.sales_id
        FROM reports r
        LEFT JOIN students s ON s.phone = r.student_id
        WHERE r.id=?
        """,
        (report_id,),
    ).fetchone()
    if not report_row:
        conn.close()
        return False

    current_phone = report_row["student_id"]
    next_phone = (phone or current_phone or "").strip()
    next_email = report_row["email"] if email is None else email.strip() if isinstance(email, str) else email
    report_payload = _deserialize_json(report_row["report_json"])
    student_payload = dict(report_payload.get("student") or {})
    student_name = report_row["name"] or student_payload.get("name") or "Student"
    student_branch = report_row["branch"] or student_payload.get("branch")
    next_sales_id = (
        sales_id
        if sales_id_provided
        else report_row["sales_id"]
        if report_row["sales_id"] is not None
        else student_payload.get("sales_id")
    )
    if next_sales_id in ("", None):
        next_sales_id = None

    cleaned_email = next_email if next_email not in ("", None) else None

    if current_phone and next_phone:
        duplicate = cursor.execute(
            "SELECT phone FROM students WHERE phone=? AND phone<>?",
            (next_phone, current_phone),
        ).fetchone()
        if duplicate:
            cursor.execute(
                """
                UPDATE students
                SET email=?,
                    sales_id=?
                WHERE phone=?
                """,
                (cleaned_email, next_sales_id, next_phone),
            )
            cursor.execute(
                "UPDATE reports SET student_id=? WHERE id=?",
                (next_phone, report_id),
            )
            conn.commit()
            conn.close()
            return True

        cursor.execute(
            """
            UPDATE students
            SET phone=?,
                email=?,
                sales_id=?
            WHERE phone=?
            """,
            (
                next_phone,
                cleaned_email,
                next_sales_id,
                current_phone,
            ),
        )
    elif next_phone:
        existing = cursor.execute(
            "SELECT phone FROM students WHERE phone=?",
            (next_phone,),
        ).fetchone()
        if existing:
            cursor.execute(
                """
                UPDATE students
                SET name=COALESCE(name, ?),
                    branch=COALESCE(branch, ?),
                    email=?,
                    sales_id=?
                WHERE phone=?
                """,
                (student_name, student_branch, cleaned_email, next_sales_id, next_phone),
            )
        else:
            cursor.execute(
                """
                INSERT INTO students (phone, name, branch, email, sales_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (next_phone, student_name, student_branch, cleaned_email, next_sales_id),
            )
        cursor.execute(
            "UPDATE reports SET student_id=? WHERE id=?",
            (next_phone, report_id),
        )
    else:
        conn.commit()
        conn.close()
        return True
    conn.commit()
    conn.close()
    return True


def list_delivery_logs(report_id):
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT *
        FROM delivery_logs
        WHERE report_id=?
        ORDER BY id DESC
        """,
        (report_id,),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def has_successful_delivery(report_id, delivery_target):
    conn = get_connection()
    row = conn.execute(
        """
        SELECT 1
        FROM delivery_logs
        WHERE report_id=? AND delivery_target=? AND status='sent'
        LIMIT 1
        """,
        (report_id, delivery_target),
    ).fetchone()
    conn.close()
    return bool(row)


def save_report_document(
    report_id,
    storage_provider,
    storage_key,
    content=None,
    content_type="application/pdf",
):
    conn = get_connection()
    conn.execute(
        """
        INSERT INTO report_documents (
            report_id,
            storage_provider,
            storage_key,
            content,
            content_type
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(report_id) DO UPDATE SET
            storage_provider=excluded.storage_provider,
            storage_key=excluded.storage_key,
            content=excluded.content,
            content_type=excluded.content_type
        """,
        (report_id, storage_provider, storage_key, content, content_type),
    )
    conn.commit()
    conn.close()


def get_report_document(report_id):
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM report_documents WHERE report_id=?",
        (report_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _deserialize_json(raw_value):
    if not raw_value:
        return {}
    try:
        return json.loads(raw_value)
    except (TypeError, json.JSONDecodeError):
        return {}


def _reset_autoincrement(cursor, table_name):
    max_id = cursor.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table_name}").fetchone()[0]
    if max_id:
        cursor.execute(
            "UPDATE sqlite_sequence SET seq=? WHERE name=?",
            (max_id, table_name),
        )
    else:
        cursor.execute("DELETE FROM sqlite_sequence WHERE name=?", (table_name,))

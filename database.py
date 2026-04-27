import json
import os
import sqlite3

from werkzeug.security import generate_password_hash

DB_NAME = os.environ.get("DB_NAME", "app.db")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "http://localhost:5000").rstrip(
    "/"
)

USER_ROLES = ("teacher", "operation", "admin", "sales", "sales_admin", "manager", "superadmin")
REPORT_TYPES = ("midterm", "final")
REPORT_STATUSES = ("draft", "pending_operation", "approved", "delivered", "rejected")
DELIVERY_CHANNELS = ("email",)
DELIVERY_STATUSES = ("pending", "sent", "failed")


def get_connection():
    conn = sqlite3.connect(DB_NAME, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF")

    _migrate_users(cursor)
    _ensure_user_indexes(cursor)
    _create_students_table(cursor)
    _migrate_reports_table(cursor)
    _migrate_report_tracking_table(cursor)
    _create_evaluations_table(cursor)
    _create_delivery_logs_table(cursor)
    _create_indexes(cursor)
    _seed_default_users(cursor)
    _cleanup_legacy_tables(cursor)

    cursor.execute("PRAGMA foreign_key_check")
    conn.commit()
    cursor.execute("PRAGMA foreign_keys = ON")
    conn.close()


def _migrate_users(cursor):
    if _users_table_is_current(cursor):
        return

    legacy_exists = _table_exists(cursor, "users")
    if legacy_exists:
        cursor.execute("ALTER TABLE users RENAME TO users_legacy")

    cursor.execute(
        f"""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
        username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE,
            password_hash TEXT,
            password_plain TEXT,
            role TEXT NOT NULL CHECK(role IN {_as_sql_tuple(USER_ROLES)}),
            branch TEXT,
            must_change_password INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users(lower(username))"
    )

    if not legacy_exists:
        return

    legacy_columns = _get_table_columns(cursor, "users_legacy")
    rows = cursor.execute("SELECT * FROM users_legacy ORDER BY id").fetchall()

    for row in rows:
        role = row["role"] if "role" in legacy_columns and row["role"] in USER_ROLES else "teacher"
        password_hash = (
            row["password_hash"]
            if "password_hash" in legacy_columns and row["password_hash"]
            else generate_password_hash("123")
        )
        is_active = row["is_active"] if "is_active" in legacy_columns else 1
        must_change_password = (
            row["must_change_password"]
            if "must_change_password" in legacy_columns
            else 0
            if role == "superadmin"
            else 1
        )
        branch = row["branch"] if "branch" in legacy_columns else None
        created_at = row["created_at"] if "created_at" in legacy_columns else None
        username = (
            row["username"]
            if "username" in legacy_columns and row["username"]
            else _unique_username(
                cursor,
                _default_username(row["email"] if "email" in legacy_columns else None),
            )
        )
        password_plain = row["password_plain"] if "password_plain" in legacy_columns else None

        cursor.execute(
            """
            INSERT INTO users (id, name, username, email, password_hash, password_plain, role, branch, must_change_password, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (
                row["id"],
                row["name"],
                username,
                row["email"] if "email" in legacy_columns else None,
                password_hash,
                password_plain,
                role,
                branch,
                must_change_password,
                is_active,
                created_at,
            ),
        )

    cursor.execute("DROP TABLE users_legacy")


def _ensure_user_indexes(cursor):
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower ON users(lower(username))"
    )


def _create_students_table(cursor):
    if _table_references_legacy(cursor, "students"):
        cursor.execute("ALTER TABLE students RENAME TO students_old_fk")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS students (
            phone TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            branch TEXT,
            email TEXT,
            sales_id INTEGER,
            FOREIGN KEY(sales_id) REFERENCES users(id)
        )
        """
    )

    if _table_exists(cursor, "students_old_fk"):
        old_columns = _get_table_columns(cursor, "students_old_fk")
        email_expr = "email" if "email" in old_columns else "NULL"
        sales_expr = (
            "CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = students_old_fk.sales_id) "
            "THEN sales_id ELSE NULL END"
            if "sales_id" in old_columns
            else "NULL"
        )
        cursor.execute(
            f"""
            INSERT OR IGNORE INTO students (phone, name, branch, email, sales_id)
            SELECT phone, name, branch, {email_expr}, {sales_expr}
            FROM students_old_fk
            """
        )
        cursor.execute("DROP TABLE students_old_fk")

    _ensure_column(cursor, "students", "email", "TEXT")


def _migrate_reports_table(cursor):
    current_schema = _reports_table_is_current(cursor)
    legacy_statuses = _load_legacy_report_statuses(cursor) if not current_schema else {}

    legacy_exists = _table_exists(cursor, "reports") and not current_schema
    if legacy_exists:
        cursor.execute("ALTER TABLE reports RENAME TO reports_legacy")

    if not _table_exists(cursor, "reports"):
        cursor.execute(
            f"""
            CREATE TABLE reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id TEXT,
                teacher_id INTEGER NOT NULL,
                report_type TEXT NOT NULL CHECK(report_type IN {_as_sql_tuple(REPORT_TYPES)}),
                level TEXT,
                course TEXT,
                report_json TEXT NOT NULL,
                pdf_path TEXT,
                status TEXT NOT NULL CHECK(status IN {_as_sql_tuple(REPORT_STATUSES)}) DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER,
                FOREIGN KEY(student_id) REFERENCES students(phone) ON UPDATE CASCADE,
                FOREIGN KEY(teacher_id) REFERENCES users(id),
                FOREIGN KEY(created_by) REFERENCES users(id)
            )
            """
        )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS report_documents (
            report_id INTEGER PRIMARY KEY,
            storage_provider TEXT NOT NULL DEFAULT 'database',
            storage_key TEXT NOT NULL UNIQUE,
            content BLOB,
            content_type TEXT NOT NULL DEFAULT 'application/pdf',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES reports(id) ON DELETE CASCADE
        )
        """
    )

    if legacy_exists:
        legacy_columns = _get_table_columns(cursor, "reports_legacy")
        rows = cursor.execute("SELECT * FROM reports_legacy ORDER BY id").fetchall()

        for row in rows:
            report_payload = _safe_json_loads(row["report_json"]) if "report_json" in legacy_columns else {}
            student_payload = report_payload.get("student", {})
            student_phone = (
                student_payload.get("phone")
                or student_payload.get("student_phone")
                or student_payload.get("mobile")
                or f"legacy-{row['id']}"
            )
            student_name = (
                row["student_name"]
                if "student_name" in legacy_columns and row["student_name"]
                else student_payload.get("name")
                or f"Legacy Student {row['id']}"
            )
            teacher_name = (
                row["teacher_name"]
                if "teacher_name" in legacy_columns and row["teacher_name"]
                else student_payload.get("teacher")
            )
            created_by = row["created_by"] if "created_by" in legacy_columns else None
            teacher_id = _resolve_teacher_id(cursor, created_by, teacher_name)
            branch = student_payload.get("branch")
            sales_id = _resolve_sales_id(cursor, student_payload.get("sales_id"))
            email = student_payload.get("email")
            level = row["level"] if "level" in legacy_columns else student_payload.get("level")
            course = row["course"] if "course" in legacy_columns else student_payload.get("course")
            report_type = _normalize_report_type(
                report_payload.get("report_type")
                or report_payload.get("meta", {}).get("report_type")
            )
            status_source = row["status"] if "status" in legacy_columns else legacy_statuses.get(row["id"])
            status = _normalize_legacy_status(status_source)
            created_at = row["created_at"] if "created_at" in legacy_columns else None

            cursor.execute(
                """
                INSERT OR IGNORE INTO students (phone, name, branch, email, sales_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (student_phone, student_name, branch, email, sales_id),
            )

            cursor.execute(
                """
                INSERT INTO reports (
                    id,
                    student_id,
                    teacher_id,
                    report_type,
                    level,
                    course,
                    report_json,
                    pdf_path,
                    status,
                    created_at,
                    created_by
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?)
                """,
                (
                    row["id"],
                    student_phone,
                    teacher_id,
                    report_type,
                    level,
                    course,
                    row["report_json"] if "report_json" in legacy_columns and row["report_json"] else json.dumps(report_payload),
                    None,
                    status,
                    created_at,
                    created_by,
                ),
            )

            if "pdf_path" in legacy_columns and row["pdf_path"]:
                _import_pdf_file(cursor, row["id"], row["pdf_path"])

        cursor.execute("DROP TABLE reports_legacy")
    else:
        _migrate_existing_pdf_references(cursor)

    _rebuild_report_documents_table(cursor)


def _migrate_report_tracking_table(cursor):
    if _tracking_table_is_current(cursor):
        _ensure_tracking_rows(cursor)
        return

    legacy_exists = _table_exists(cursor, "report_tracking")
    if legacy_exists:
        cursor.execute("ALTER TABLE report_tracking RENAME TO report_tracking_legacy")

    cursor.execute(
        f"""
        CREATE TABLE report_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER NOT NULL,
            status TEXT NOT NULL CHECK(status IN {_as_sql_tuple(REPORT_STATUSES)}),
            changed_by INTEGER,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES reports(id) ON DELETE CASCADE,
            FOREIGN KEY(changed_by) REFERENCES users(id)
        )
        """
    )

    if legacy_exists:
        legacy_columns = _get_table_columns(cursor, "report_tracking_legacy")
        rows = cursor.execute(
            "SELECT * FROM report_tracking_legacy ORDER BY id"
        ).fetchall()

        for row in rows:
            if not _report_exists(cursor, row["report_id"]):
                continue

            legacy_status = row["status"] if "status" in legacy_columns else None
            status = _normalize_legacy_status(legacy_status)
            if "changed_by" in legacy_columns:
                changed_by = row["changed_by"]
            else:
                changed_by = row["assigned_to"] if "assigned_to" in legacy_columns else None
            notes = row["notes"] if "notes" in legacy_columns else None
            if "created_at" in legacy_columns:
                created_at = row["created_at"]
            else:
                created_at = row["sent_at"] if "sent_at" in legacy_columns else None

            cursor.execute(
                """
                INSERT INTO report_tracking (report_id, status, changed_by, notes, created_at)
                VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """,
                (row["report_id"], status, changed_by, notes, created_at),
            )

        cursor.execute("DROP TABLE report_tracking_legacy")

    _ensure_tracking_rows(cursor)


def _create_evaluations_table(cursor):
    if _table_references_legacy(cursor, "evaluations"):
        cursor.execute("ALTER TABLE evaluations RENAME TO evaluations_old_fk")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER NOT NULL UNIQUE,
            teacher_id INTEGER NOT NULL,
            sales_id INTEGER NOT NULL,
            rating_teacher INTEGER,
            rating_sales INTEGER,
            teacher_notes TEXT,
            sales_notes TEXT,
            created_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES reports(id) ON DELETE CASCADE,
            FOREIGN KEY(teacher_id) REFERENCES users(id),
            FOREIGN KEY(sales_id) REFERENCES users(id),
            FOREIGN KEY(created_by) REFERENCES users(id)
        )
        """
    )

    if _table_exists(cursor, "evaluations_old_fk"):
        cursor.execute(
            """
            INSERT OR IGNORE INTO evaluations (
                id, report_id, teacher_id, sales_id, rating_teacher, rating_sales,
                teacher_notes, sales_notes, created_by, created_at
            )
            SELECT
                old.id,
                old.report_id,
                old.teacher_id,
                old.sales_id,
                old.rating_teacher,
                old.rating_sales,
                old.teacher_notes,
                old.sales_notes,
                CASE WHEN EXISTS (SELECT 1 FROM users WHERE users.id = old.created_by)
                     THEN old.created_by ELSE NULL END,
                old.created_at
            FROM evaluations_old_fk old
            WHERE EXISTS (SELECT 1 FROM reports WHERE reports.id = old.report_id)
              AND EXISTS (SELECT 1 FROM users WHERE users.id = old.teacher_id)
              AND EXISTS (SELECT 1 FROM users WHERE users.id = old.sales_id)
            """
        )
        cursor.execute("DROP TABLE evaluations_old_fk")


def _create_delivery_logs_table(cursor):
    if _table_references_legacy(cursor, "delivery_logs"):
        cursor.execute("ALTER TABLE delivery_logs RENAME TO delivery_logs_old_fk")

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS delivery_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER NOT NULL,
            channel TEXT NOT NULL CHECK(channel IN {_as_sql_tuple(DELIVERY_CHANNELS)}),
            delivery_target TEXT,
            recipient TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN {_as_sql_tuple(DELIVERY_STATUSES)}) DEFAULT 'pending',
            error_message TEXT,
            sent_at TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES reports(id) ON DELETE CASCADE
        )
        """
    )

    if _table_exists(cursor, "delivery_logs_old_fk"):
        old_columns = _get_table_columns(cursor, "delivery_logs_old_fk")
        delivery_target_expr = "delivery_target" if "delivery_target" in old_columns else "NULL"
        cursor.execute(
            f"""
            INSERT OR IGNORE INTO delivery_logs (
                id, report_id, channel, delivery_target, recipient, status, error_message, sent_at
            )
            SELECT
                id,
                report_id,
                channel,
                {delivery_target_expr},
                recipient,
                status,
                error_message,
                sent_at
            FROM delivery_logs_old_fk
            WHERE EXISTS (SELECT 1 FROM reports WHERE reports.id = delivery_logs_old_fk.report_id)
            """
        )
        cursor.execute("DROP TABLE delivery_logs_old_fk")

    _ensure_column(cursor, "delivery_logs", "delivery_target", "TEXT")


def _create_indexes(cursor):
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_students_sales_id ON students(sales_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_reports_student_id ON reports(student_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_reports_teacher_id ON reports(teacher_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_tracking_report_id ON report_tracking(report_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_delivery_logs_report_id ON delivery_logs(report_id)"
    )


def _seed_default_users(cursor):
    username_aliases = {
        "teacher.hussam": "hussam",
        "teacher.fouad": "fouad",
        "teacher.sara": "sara",
        "teacher.sabrina": "sabrina",
        "teacher.fatima": "fatima",
        "teacher.somaya": "somaya",
        "teacher.nourhan": "nourhan",
        "teacher.rama": "rama",
        "teacher.rose": "rose",
        "sales.sarah": "sarah",
        "sales.israa": "israa",
        "sales.hajar": "hajar",
        "sales.mai": "mai",
        "sales.somar": "somar",
        "sales.mazen": "mazen",
        "sales.zain": "zain",
        "sales.amin": "amin",
        "sales.rana": "rana",
        "sales.karam": "karam",
        "sales.omar": "omar",
    }
    for old_username, new_username in username_aliases.items():
        old_user = cursor.execute(
            "SELECT id FROM users WHERE lower(username)=lower(?)",
            (old_username,),
        ).fetchone()
        if not old_user:
            continue
        new_user = cursor.execute(
            "SELECT id FROM users WHERE lower(username)=lower(?)",
            (new_username,),
        ).fetchone()
        if new_user:
            cursor.execute("UPDATE users SET is_active=0 WHERE id=?", (old_user["id"],))
        else:
            cursor.execute("UPDATE users SET username=? WHERE id=?", (new_username, old_user["id"]))

    seeds = [
        ("Samer", "samer", None, "S@mer-2026!R9p#L4", "superadmin", None),
        ("Haider", "haider", None, "123", "manager", "AlAin"),
        ("Admin", "admin", "admin@test.com", "123", "admin", "AlAin"),
        ("Operation", "operation", "op@test.com", "123", "operation", "AlAin"),
        ("Sarah Admin", "SarahAdmin", None, "123", "sales_admin", "AlAin"),
        ("Hussam", "hussam", None, "123", "teacher", "AlAin"),
        ("Fouad", "fouad", None, "123", "teacher", "AlAin"),
        ("Sara", "sara", None, "123", "teacher", "AlAin"),
        ("Sabrina", "sabrina", None, "123", "teacher", "AlAin"),
        ("Fatima", "fatima", None, "123", "teacher", "AlAin"),
        ("Somaya", "somaya", None, "123", "teacher", "AlAin"),
        ("Nourhan", "nourhan", None, "123", "teacher", "AlAin"),
        ("Rama", "rama", None, "123", "teacher", "AlAin"),
        ("Rose", "rose", None, "123", "teacher", "AlAin"),
        ("Sarah", "sarah", None, "123", "sales", "AlAin"),
        ("Israa", "israa", None, "123", "sales", "AlAin"),
        ("Hajar", "hajar", None, "123", "sales", "AlAin"),
        ("Mai", "mai", None, "123", "sales", "AlAin"),
        ("Somar", "somar", None, "123", "sales", "AlAin"),
        ("Mazen", "mazen", None, "123", "sales", "AlAin"),
        ("Zain", "zain", None, "123", "sales", "AlAin"),
        ("Amin", "amin", None, "123", "sales", "AlAin"),
        ("Rana", "rana", None, "123", "sales", "AlAin"),
        ("Karam", "karam", None, "123", "sales", "AlAin"),
        ("Omar", "omar", None, "123", "sales", "AlAin"),
        ("Admin Abu Dhabi", "adminabudhabi", "admin.abudhabi@test.com", "123", "admin", "Abu Dhabi"),
        ("Operation Abu Dhabi", "operationabudhabi", "operation.abudhabi@test.com", "123", "operation", "Abu Dhabi"),
    ]

    for name, username, email, password, role, branch in seeds:
        existing = cursor.execute(
            "SELECT id FROM users WHERE lower(username)=lower(?) OR (email IS NOT NULL AND email=?)",
            (username, email),
        ).fetchone()
        if existing:
            username_taken = cursor.execute(
                "SELECT id FROM users WHERE lower(username)=lower(?) AND id<>?",
                (username, existing["id"]),
            ).fetchone()
            if not username_taken:
                cursor.execute(
                    """
                    UPDATE users
                    SET username=?,
                        password_plain=NULL
                    WHERE id=?
                    """,
                    (username, existing["id"]),
                )
            continue

        cursor.execute(
            """
            INSERT INTO users (name, username, email, password_hash, password_plain, role, branch, must_change_password, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                name,
                username,
                email,
                generate_password_hash(password),
                None,
                role,
                branch,
                0 if role == "superadmin" else 1,
            ),
        )

    cursor.execute("UPDATE users SET password_plain=NULL")
    cursor.execute("UPDATE users SET is_active=0 WHERE role='superadmin' AND lower(username) <> 'samer'")
    cursor.execute(
        """
        UPDATE users
        SET is_active=0
        WHERE lower(username) IN (
            'teacheralain',
            'teacherabudhabi',
            'salesalain',
            'salesabudhabi'
        )
        """
    )


def _cleanup_legacy_tables(cursor):
    for table_name in ("users_legacy", "reports_legacy", "report_tracking_legacy"):
        if _table_exists(cursor, table_name):
            cursor.execute(f"DROP TABLE {table_name}")


def _users_table_is_current(cursor):
    if not _table_exists(cursor, "users"):
        return False

    columns = _get_table_columns(cursor, "users")
    sql = (_get_table_sql(cursor, "users") or "").lower()
    return (
        {"id", "name", "username", "email", "password_hash", "password_plain", "role", "branch", "must_change_password", "is_active", "created_at"}
        .issubset(columns)
        and "sales" in sql
        and "sales_admin" in sql
        and "manager" in sql
        and "superadmin" in sql
    )


def _reports_table_is_current(cursor):
    if not _table_exists(cursor, "reports"):
        return False

    columns = _get_table_columns(cursor, "reports")
    sql = (_get_table_sql(cursor, "reports") or "").lower()
    student_column = None
    for row in cursor.execute("PRAGMA table_info(reports)").fetchall():
        if row[1] == "student_id":
            student_column = row
            break

    return (
        {
        "id",
        "student_id",
        "teacher_id",
        "report_type",
        "level",
        "course",
        "report_json",
        "pdf_path",
        "status",
        "created_at",
        "created_by",
        }.issubset(columns)
        and student_column is not None
        and student_column[3] == 0
        and "rejected" in sql
        and not _table_references_legacy(cursor, "reports")
    )


def _tracking_table_is_current(cursor):
    if not _table_exists(cursor, "report_tracking"):
        return False

    columns = _get_table_columns(cursor, "report_tracking")
    sql = (_get_table_sql(cursor, "report_tracking") or "").lower()
    return {
        "id",
        "report_id",
        "status",
        "changed_by",
        "notes",
        "created_at",
    }.issubset(columns) and "rejected" in sql and not _table_references_legacy(cursor, "report_tracking")


def _migrate_existing_pdf_references(cursor):
    if not _table_exists(cursor, "report_documents"):
        return

    rows = cursor.execute("SELECT id, pdf_path FROM reports").fetchall()
    for row in rows:
        if _report_document_exists(cursor, row["id"]):
            if not row["pdf_path"]:
                cursor.execute(
                    "UPDATE reports SET pdf_path=? WHERE id=?",
                    (_public_pdf_url(row["id"]), row["id"]),
                )
            continue

        pdf_path = row["pdf_path"]
        if pdf_path and os.path.exists(pdf_path):
            _import_pdf_file(cursor, row["id"], pdf_path)


def _rebuild_report_documents_table(cursor):
    if not _table_references_legacy(cursor, "report_documents"):
        return

    cursor.execute("ALTER TABLE report_documents RENAME TO report_documents_old_fk")
    cursor.execute(
        """
        CREATE TABLE report_documents (
            report_id INTEGER PRIMARY KEY,
            storage_provider TEXT NOT NULL DEFAULT 'database',
            storage_key TEXT NOT NULL UNIQUE,
            content BLOB,
            content_type TEXT NOT NULL DEFAULT 'application/pdf',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(report_id) REFERENCES reports(id) ON DELETE CASCADE
        )
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO report_documents (
            report_id, storage_provider, storage_key, content, content_type, created_at
        )
        SELECT
            old.report_id,
            old.storage_provider,
            old.storage_key,
            old.content,
            old.content_type,
            old.created_at
        FROM report_documents_old_fk old
        WHERE EXISTS (SELECT 1 FROM reports WHERE reports.id = old.report_id)
        """
    )
    cursor.execute("DROP TABLE report_documents_old_fk")


def _import_pdf_file(cursor, report_id, pdf_path):
    try:
        with open(pdf_path, "rb") as handle:
            content = handle.read()
    except OSError:
        return

    storage_key = f"reports/{report_id}.pdf"
    cursor.execute(
        """
        INSERT OR REPLACE INTO report_documents (
            report_id,
            storage_provider,
            storage_key,
            content,
            content_type
        )
        VALUES (?, 'database', ?, ?, 'application/pdf')
        """,
        (report_id, storage_key, content),
    )
    cursor.execute(
        "UPDATE reports SET pdf_path=? WHERE id=?",
        (_public_pdf_url(report_id), report_id),
    )


def _load_legacy_report_statuses(cursor):
    if not _table_exists(cursor, "report_tracking"):
        return {}

    columns = _get_table_columns(cursor, "report_tracking")
    if {"report_id", "status"}.issubset(columns) and "changed_by" not in columns:
        rows = cursor.execute(
            """
            SELECT report_id, status
            FROM report_tracking
            ORDER BY id
            """
        ).fetchall()
        return {
            row["report_id"]: _normalize_legacy_status(row["status"])
            for row in rows
        }

    return {}


def _ensure_tracking_rows(cursor):
    reports = cursor.execute("SELECT id, status, created_by, created_at FROM reports").fetchall()
    for report in reports:
        exists = cursor.execute(
            "SELECT 1 FROM report_tracking WHERE report_id=? LIMIT 1", (report["id"],)
        ).fetchone()
        if exists:
            continue

        cursor.execute(
            """
            INSERT INTO report_tracking (report_id, status, changed_by, notes, created_at)
            VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (
                report["id"],
                report["status"] or "draft",
                report["created_by"],
                "Initial state",
                report["created_at"],
            ),
        )


def _resolve_teacher_id(cursor, created_by, teacher_name):
    if created_by and _user_has_role(cursor, created_by, ("teacher", "admin")):
        return created_by

    if teacher_name:
        row = cursor.execute(
            "SELECT id FROM users WHERE name=? AND role='teacher' ORDER BY id LIMIT 1",
            (teacher_name,),
        ).fetchone()
        if row:
            return row["id"]

        cursor.execute(
            """
            INSERT INTO users (name, username, email, password_hash, password_plain, role, branch, is_active)
            VALUES (?, ?, ?, ?, ?, 'teacher', NULL, 1)
            """,
            (
                teacher_name,
                _unique_username(cursor, _username_from_email(_legacy_email_stub(teacher_name))),
                _legacy_email_stub(teacher_name),
                generate_password_hash("123"),
                "123",
            ),
        )
        return cursor.lastrowid

    row = cursor.execute(
        "SELECT id FROM users WHERE role='teacher' ORDER BY id LIMIT 1"
    ).fetchone()
    if row:
        return row["id"]

    cursor.execute(
        """
        INSERT INTO users (name, username, email, password_hash, password_plain, role, branch, is_active)
        VALUES ('Teacher', ?, ?, ?, ?, 'teacher', NULL, 1)
        """,
        (
            _unique_username(cursor, _username_from_email(_legacy_email_stub("teacher"))),
            _legacy_email_stub("teacher"),
            generate_password_hash("123"),
            "123",
        ),
    )
    return cursor.lastrowid


def _resolve_sales_id(cursor, sales_id):
    if sales_id and _user_has_role(cursor, sales_id, ("sales",)):
        return sales_id
    row = cursor.execute(
        "SELECT id FROM users WHERE role='sales' AND is_active=1 ORDER BY id LIMIT 1"
    ).fetchone()
    return row["id"] if row else None


def _user_has_role(cursor, user_id, roles):
    row = cursor.execute("SELECT role FROM users WHERE id=?", (user_id,)).fetchone()
    return bool(row and row["role"] in roles)


def _report_exists(cursor, report_id):
    row = cursor.execute("SELECT 1 FROM reports WHERE id=?", (report_id,)).fetchone()
    return bool(row)


def _report_document_exists(cursor, report_id):
    row = cursor.execute(
        "SELECT 1 FROM report_documents WHERE report_id=?", (report_id,)
    ).fetchone()
    return bool(row)


def _normalize_report_type(value):
    if value in REPORT_TYPES:
        return value
    return "midterm"


def _normalize_legacy_status(value):
    mapping = {
        "pending": "pending_operation",
        "sent": "delivered",
        "draft": "draft",
        "pending_operation": "pending_operation",
        "approved": "approved",
        "delivered": "delivered",
        "rejected": "rejected",
    }
    return mapping.get(value, "draft")


def _public_pdf_url(report_id):
    return f"{PUBLIC_BASE_URL}/storage/reports/{report_id}/pdf"


def _legacy_email_stub(name):
    normalized = "".join(ch.lower() if ch.isalnum() else "." for ch in name).strip(".")
    normalized = normalized or "legacy.user"
    return f"{normalized}@legacy.local"


def _username_from_email(email):
    base = (email or "").split("@", 1)[0]
    base = "".join(ch.lower() if ch.isalnum() else "_" for ch in base).strip("_")
    return base or "user"


def _default_username(email):
    email = (email or "").lower()
    if email == "admin@test.com":
        return "admin"
    if email == "op@test.com":
        return "operation"
    return _username_from_email(email)


def _unique_username(cursor, username):
    base = username or "user"
    candidate = base
    counter = 2
    while cursor.execute(
        "SELECT 1 FROM users WHERE lower(username)=lower(?)", (candidate,)
    ).fetchone():
        candidate = f"{base}{counter}"
        counter += 1
    return candidate


def _safe_json_loads(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}


def _table_exists(cursor, table):
    row = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return bool(row)


def _table_references_legacy(cursor, table):
    sql = (_get_table_sql(cursor, table) or "").lower()
    return (
        "users_legacy" in sql
        or "reports_legacy" in sql
        or "_old_fk" in sql
    )


def _get_table_columns(cursor, table):
    return {row[1] for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()}


def _get_table_sql(cursor, table):
    row = cursor.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row["sql"] if row else None


def _as_sql_tuple(values):
    return "(" + ", ".join(f"'{value}'" for value in values) + ")"


def _ensure_column(cursor, table_name, column_name, definition):
    columns = _get_table_columns(cursor, table_name)
    if column_name not in columns:
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"
        )

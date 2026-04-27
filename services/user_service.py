import re

from werkzeug.security import check_password_hash, generate_password_hash

import models

from . import ServiceError

VALID_ROLES = {"teacher", "operation", "admin", "sales", "sales_admin", "manager", "superadmin"}
OWNER_MANAGED_ROLES = {"teacher", "operation", "admin", "sales", "sales_admin", "manager"}
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def authenticate_user(username, password):
    user = models.get_user_by_username_ci(username)
    if not user or not user.get("is_active"):
        return None
    if not check_password_hash(user["password_hash"], password):
        return None
    return user


def get_user(user_id):
    return models.get_user_by_id(user_id)


def list_users(role=None, branch=None):
    users = models.list_users(role=role, branch=branch)
    for user in users:
        user["name"] = _format_name_words(user.get("name"))
        user.pop("password_hash", None)
        user.pop("password_plain", None)
        user.pop("email", None)
    return users


def list_visible_users(actor, role=None, branch=None):
    if user_has_role(actor, "superadmin"):
        selected_role = role or None
        if selected_role == "superadmin":
            users = []
        else:
            users = models.list_users(
                role=selected_role,
                branch=branch or None,
                exclude_roles=("superadmin",),
            )
    elif user_has_role(actor, "manager"):
        selected_role = role or None
        if selected_role in {"teacher", "operation", "admin", "sales", "sales_admin"}:
            users = models.list_users(role=selected_role, branch=branch or None)
        else:
            users = []
    elif user_has_role(actor, "admin"):
        selected_role = role or None
        if selected_role in {"teacher", "operation", "sales"}:
            users = models.list_users(role=selected_role, branch=actor.get("branch"))
        else:
            users = []
    elif user_has_role(actor, "teacher", "operation"):
        users = models.list_users(
            role=role or "sales",
            branch=actor.get("branch"),
            exclude_roles=("admin", "superadmin"),
        )
    else:
        users = []

    for user in users:
        user.pop("password_hash", None)
        user.pop("password_plain", None)
        user.pop("email", None)
    return users


def create_user_account(actor, name, username, password, role, branch=None):
    if role not in VALID_ROLES:
        raise ServiceError("Invalid role", 400)
    if role == "superadmin":
        raise ServiceError("Owner accounts cannot be created from this page", 403)
    if not password:
        raise ServiceError("Password is required", 400)
    if not username:
        raise ServiceError("Username is required", 400)
    if not USERNAME_PATTERN.fullmatch(username):
        raise ServiceError("Username can only contain letters, numbers, dots, underscores, and hyphens", 400)

    if not user_has_role(actor, "superadmin"):
        raise ServiceError("Forbidden", 403)
    if role not in OWNER_MANAGED_ROLES:
        raise ServiceError("Invalid role", 400)
    allowed_branch = branch or None
    if not allowed_branch:
        raise ServiceError("Branch is required", 400)

    existing_username = models.get_user_by_username_ci(username)
    if existing_username:
        raise ServiceError("Username already exists", 409)

    user_id = models.create_user(
        name=name,
        username=username,
        password_hash=generate_password_hash(password),
        role=role,
        branch=allowed_branch,
        password_plain=None,
        must_change_password=1,
    )
    user = models.get_user_by_id(user_id)
    user.pop("password_hash", None)
    user.pop("password_plain", None)
    user.pop("email", None)
    return user


def reset_user_password(actor, account_password, user_id, new_password):
    if not user_has_role(actor, "superadmin"):
        raise ServiceError("Only owner can reset passwords", 403)
    if not check_password_hash(actor["password_hash"], account_password):
        raise ServiceError("Owner password is incorrect", 403)
    if not user_id:
        raise ServiceError("User is required", 400)
    if not new_password:
        raise ServiceError("New password is required", 400)

    user = models.get_user_by_id(user_id)
    if not user or not user.get("is_active"):
        raise ServiceError("User not found", 404)
    if user.get("role") == "superadmin":
        raise ServiceError("Owner password cannot be reset from this page", 403)
    if not models.reset_user_password(user["id"], generate_password_hash(new_password)):
        raise ServiceError("User not found", 404)

    return {
        "id": user["id"],
        "reset": True,
    }


def change_own_password(actor, new_password, confirm_password):
    if not actor:
        raise ServiceError("Your session expired. Please log in again.", 401)
    if not new_password:
        raise ServiceError("New password is required", 400)
    if new_password != confirm_password:
        raise ServiceError("Passwords do not match", 400)
    if len(new_password) < 3:
        raise ServiceError("Password must be at least 3 characters", 400)

    if not models.change_user_password(actor["id"], generate_password_hash(new_password)):
        raise ServiceError("User not found", 404)
    return {"changed": True}


def delete_user_account(actor, account_password, user_id):
    if not user_has_role(actor, "superadmin"):
        raise ServiceError("Only owner can delete users", 403)
    if not check_password_hash(actor["password_hash"], account_password):
        raise ServiceError("Account password is incorrect", 403)
    if not user_id:
        raise ServiceError("User is required", 400)

    try:
        user_id = int(user_id)
    except (TypeError, ValueError) as exc:
        raise ServiceError("Invalid user", 400) from exc

    if user_id == actor.get("id"):
        raise ServiceError("You cannot delete your own account", 403)

    user = models.get_user_by_id(user_id)
    if not user or not user.get("is_active"):
        raise ServiceError("User not found", 404)

    if user.get("role") == "superadmin":
        raise ServiceError("You cannot delete this user", 403)

    if not models.deactivate_user(user_id):
        raise ServiceError("User not found", 404)

    return {"id": user_id, "deleted": True}


def update_user_branch(actor, user_id, branch):
    if not user_has_role(actor, "superadmin"):
        raise ServiceError("Only owner can update branches", 403)
    if not branch:
        raise ServiceError("Branch is required", 400)

    user = models.get_user_by_id(user_id)
    if not user or not user.get("is_active"):
        raise ServiceError("User not found", 404)
    if user.get("role") == "superadmin":
        raise ServiceError("Owner branch cannot be updated from this page", 403)
    if user.get("role") not in OWNER_MANAGED_ROLES:
        raise ServiceError("Invalid user role", 400)
    if not models.update_user_branch(user["id"], branch):
        raise ServiceError("User not found", 404)
    updated = models.get_user_by_id(user["id"])
    updated.pop("password_hash", None)
    updated.pop("password_plain", None)
    updated.pop("email", None)
    return updated


def user_has_role(user, *roles):
    return bool(user and (user.get("role") == "superadmin" or user.get("role") in roles))


def _format_name_words(name):
    return " ".join(_format_name_token(part) for part in str(name or "").split())


def _format_name_token(token):
    return "-".join(piece[:1].upper() + piece[1:].lower() if piece else "" for piece in token.split("-"))

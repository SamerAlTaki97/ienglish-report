import re

from werkzeug.security import check_password_hash, generate_password_hash

import models

from . import ServiceError

VALID_ROLES = {"teacher", "operation", "admin", "sales", "superadmin"}
BRANCH_ADMIN_MANAGED_ROLES = {"teacher", "operation", "sales"}
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def authenticate_user(username, password):
    user = models.get_user_by_username(username)
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
        user.pop("password_hash", None)
        user.pop("password_plain", None)
        user.pop("email", None)
    return users


def list_visible_users(actor, role=None, branch=None):
    if user_has_role(actor, "superadmin"):
        users = models.list_users(role=role or None, branch=branch or None)
    elif user_has_role(actor, "admin"):
        selected_role = role or None
        if selected_role in {"admin", "superadmin"}:
            users = []
        else:
            users = models.list_users(
                role=selected_role,
                branch=actor.get("branch"),
                exclude_roles=("admin", "superadmin"),
            )
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
    if not password:
        raise ServiceError("Password is required", 400)
    if not username:
        raise ServiceError("Username is required", 400)
    if not USERNAME_PATTERN.fullmatch(username):
        raise ServiceError("Username can only contain letters, numbers, dots, underscores, and hyphens", 400)

    if user_has_role(actor, "superadmin"):
        if role == "superadmin":
            raise ServiceError("Cannot create another system manager account from this page", 403)
        allowed_branch = branch or None
        if not allowed_branch:
            raise ServiceError("Branch is required", 400)
    elif user_has_role(actor, "admin"):
        if role not in BRANCH_ADMIN_MANAGED_ROLES:
            raise ServiceError("Branch admin cannot create admin accounts", 403)
        allowed_branch = actor.get("branch")
    else:
        raise ServiceError("Forbidden", 403)

    existing_username = models.get_user_by_username_ci(username)
    if existing_username:
        raise ServiceError("Username already exists", 409)

    user_id = models.create_user(
        name=name,
        username=username,
        password_hash=generate_password_hash(password),
        role=role,
        branch=allowed_branch,
        password_plain=password,
    )
    user = models.get_user_by_id(user_id)
    user.pop("password_hash", None)
    user.pop("password_plain", None)
    user.pop("email", None)
    return user


def reveal_user_password(admin_user, admin_password, user_id):
    if not user_has_role(admin_user, "admin", "superadmin"):
        raise ServiceError("Only admin can reveal passwords", 403)
    if not check_password_hash(admin_user["password_hash"], admin_password):
        raise ServiceError("Admin password is incorrect", 403)
    if not user_id:
        raise ServiceError("User is required", 400)

    user = models.get_user_by_id(user_id)
    if not user:
        raise ServiceError("User not found", 404)
    if user_has_role(admin_user, "superadmin"):
        allowed = user.get("role") != "superadmin"
    else:
        allowed = (
            user.get("role") in BRANCH_ADMIN_MANAGED_ROLES
            and user.get("branch") == admin_user.get("branch")
        )
    if not allowed:
        raise ServiceError("You cannot reveal this user's password", 403)

    return {
        "id": user["id"],
        "password": user.get("password_plain") or "Not available",
    }


def delete_user_account(actor, account_password, user_id):
    if not user_has_role(actor, "admin", "superadmin"):
        raise ServiceError("Only admin can delete users", 403)
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

    if user_has_role(actor, "superadmin"):
        allowed = user.get("role") != "superadmin"
    else:
        allowed = (
            user.get("role") in BRANCH_ADMIN_MANAGED_ROLES
            and user.get("branch") == actor.get("branch")
        )
    if not allowed:
        raise ServiceError("You cannot delete this user", 403)

    if not models.deactivate_user(user_id):
        raise ServiceError("User not found", 404)

    return {"id": user_id, "deleted": True}


def user_has_role(user, *roles):
    return bool(user and (user.get("role") == "superadmin" or user.get("role") in roles))

import os


def _load_local_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


_load_local_env()


class AppConfig:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev_secret_change_me")
    AI_SERVICE_KEY = os.environ.get("AI_SERVICE_KEY")
    AI_TEXT_MODEL = os.environ.get("AI_TEXT_MODEL")
    PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL")
    EMAIL_LOGO_URL = os.environ.get("EMAIL_LOGO_URL")
    STORAGE_PROVIDER = os.environ.get("STORAGE_PROVIDER", "database")
    S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
    S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL")
    S3_PUBLIC_BASE_URL = os.environ.get("S3_PUBLIC_BASE_URL")
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.environ.get("AWS_REGION")

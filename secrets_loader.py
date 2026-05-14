"""
secrets_loader.py
─────────────────
Loads secrets from AWS Secrets Manager into the app at startup.
Uses EC2 IAM Role — no hardcoded AWS credentials needed.

Secret layout in AWS Secrets Manager:
  wiziagent/meta              → API keys, Slack, OpenAI, etc.
  wiziagent/db/default        → primary Redshift (backward compatible)
  wiziagent/db/<key>          → any additional datasource

Each wiziagent/db/* secret must have a "type" field:
  redshift | postgres | mysql | bigquery | snowflake
"""

import boto3, json, os, logging

logger = logging.getLogger(__name__)

AWS_REGION       = os.getenv("AWS_REGION", "us-east-1")
SECRET_META      = os.getenv("AWS_SECRET_META", "wiziagent/meta")
SECRET_DB_PREFIX = os.getenv("AWS_SECRET_DB_PREFIX", "wiziagent/db/")

# Populated at startup, shared with datasources.py via get_registry()
DB_REGISTRY: dict = {}


def _client():
    return boto3.client("secretsmanager", region_name=AWS_REGION)


def _get_secret(client, name: str) -> dict:
    try:
        resp = client.get_secret_value(SecretId=name)
        return json.loads(resp["SecretString"])
    except Exception as e:
        logger.warning(f"[secrets_loader] Could not fetch '{name}': {e}")
        return {}


def _list_db_secrets(client) -> list:
    names = []
    try:
        paginator = client.get_paginator("list_secrets")
        for page in paginator.paginate(
            Filters=[{"Key": "name", "Values": [SECRET_DB_PREFIX]}]
        ):
            for s in page.get("SecretList", []):
                names.append(s["Name"])
    except Exception as e:
        logger.warning(f"[secrets_loader] Could not list DB secrets: {e}")
    return names


def _default_port(db_type: str) -> int:
    return {"redshift": 5439, "postgres": 5432, "mysql": 3306}.get(db_type, 5432)


def load_secrets():
    """Call once at app startup before load_dotenv()."""
    global DB_REGISTRY
    try:
        c = _client()

        # 1. Meta secrets → os.environ (API keys, Slack, OpenAI, etc.)
        meta = _get_secret(c, SECRET_META)
        for k, v in meta.items():
            os.environ.setdefault(k, str(v))
        logger.info(f"[secrets_loader] Loaded {len(meta)} meta secrets")

        # 2. DB secrets → DB_REGISTRY
        for secret_name in _list_db_secrets(c):
            # wiziagent/db/client-acme → client_acme
            key = secret_name.replace(SECRET_DB_PREFIX, "").strip("/").replace("-", "_")
            data = _get_secret(c, secret_name)
            if not data:
                continue

            db_type = data.get("type", "redshift").lower()
            entry = {"type": db_type, "_secret_name": secret_name}

            if db_type == "bigquery":
                entry.update({
                    "credentials_json": data.get("credentials_json", ""),
                    "project":          data.get("project", ""),
                    "dataset":          data.get("dataset", ""),
                })
            elif db_type == "snowflake":
                entry.update({
                    "account":   data.get("account", ""),
                    "user":      data.get("user", ""),
                    "password":  data.get("password", ""),
                    "database":  data.get("database", ""),
                    "schema":    data.get("schema", "public"),
                    "warehouse": data.get("warehouse", ""),
                    "role":      data.get("role", ""),
                })
            else:
                # redshift / postgres / mysql — all use host/port/dbname/user/password
                entry.update({
                    "host":     data.get("host", ""),
                    "port":     int(data.get("port", _default_port(db_type))),
                    "dbname":   data.get("dbname", ""),
                    "user":     data.get("user", ""),
                    "password": data.get("password", ""),
                })

            DB_REGISTRY[key] = entry
            logger.info(f"[secrets_loader] Registered '{key}' (type={db_type})")

        logger.info(f"[secrets_loader] Total datasources: {len(DB_REGISTRY)}")

    except Exception as e:
        logger.warning(f"[secrets_loader] Failed: {e} — falling back to .env")


def get_registry() -> dict:
    """Called by datasources.py to access the populated registry."""
    return DB_REGISTRY
# Slack — loaded from env (set in .env locally, wiziagent/meta on AWS)
SLACK_APP_TOKEN   = os.getenv("SLACK_APP_TOKEN", "")
SLACK_BOT_TOKEN   = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_CHANNEL_ID  = os.getenv("SLACK_CHANNEL_ID", "")
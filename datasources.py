"""
datasources.py
──────────────
Multi-datasource connection factory for WiziAgent.
Supports: Redshift, PostgreSQL, MySQL, BigQuery, Snowflake.

Exposes:
  get_source_connection(key)        → raw DB connection object
  execute_on_source(key, sql, p)    → list of dicts (rows)
  router                            → FastAPI APIRouter with /api/datasources/* endpoints

Auto-registers all sources from secrets_loader.DB_REGISTRY at import time.
"""

import json, os, logging, time
from typing import Any
from fastapi import APIRouter
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Import registry from secrets_loader ───────────────────────────────────────
try:
    from secrets_loader import get_registry
    _REGISTRY = get_registry()
except ImportError:
    logger.warning("[datasources] secrets_loader not found — registry empty")
    _REGISTRY = {}


# ─────────────────────────────────────────────────────────────────────────────
# Connection factories
# ─────────────────────────────────────────────────────────────────────────────

def _connect_redshift(cfg: dict):
    import psycopg2
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
        user=cfg["user"], password=cfg["password"], sslmode="require",
        connect_timeout=10,
    )

def _connect_postgres(cfg: dict):
    import psycopg2
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
        user=cfg["user"], password=cfg["password"],
        connect_timeout=10,
    )

def _connect_mysql(cfg: dict):
    import pymysql
    return pymysql.connect(
        host=cfg["host"], port=cfg["port"], database=cfg["dbname"],
        user=cfg["user"], password=cfg["password"],
        connect_timeout=10, cursorclass=pymysql.cursors.DictCursor,
    )

def _connect_bigquery(cfg: dict):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    creds_dict = json.loads(cfg["credentials_json"])
    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(project=cfg.get("project"), credentials=credentials)

def _connect_snowflake(cfg: dict):
    import snowflake.connector
    opts = dict(
        account=cfg["account"], user=cfg["user"], password=cfg["password"],
        database=cfg["database"], schema=cfg.get("schema", "public"),
        warehouse=cfg.get("warehouse"), role=cfg.get("role"),
        login_timeout=15,
    )
    return snowflake.connector.connect(**{k: v for k, v in opts.items() if v})


_FACTORIES = {
    "redshift":  _connect_redshift,
    "postgres":  _connect_postgres,
    "mysql":     _connect_mysql,
    "bigquery":  _connect_bigquery,
    "snowflake": _connect_snowflake,
}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_source_connection(key: str = "default") -> Any:
    """
    Returns a live connection for the given datasource key.
    Raises ValueError if key is unknown or type is unsupported.
    """
    cfg = _REGISTRY.get(key)
    if not cfg:
        # Backward compat: fall back to env-based Redshift for 'default'
        if key == "default":
            return _connect_redshift({
                "host":     os.getenv("REDSHIFT_HOST"),
                "port":     int(os.getenv("REDSHIFT_PORT", 5439)),
                "dbname":   os.getenv("REDSHIFT_DB"),
                "user":     os.getenv("REDSHIFT_USER"),
                "password": os.getenv("REDSHIFT_PASSWORD"),
            })
        raise ValueError(f"Unknown datasource key: '{key}'")

    db_type = cfg.get("type", "redshift")
    factory = _FACTORIES.get(db_type)
    if not factory:
        raise ValueError(f"Unsupported DB type: '{db_type}'")

    return factory(cfg)


def execute_on_source(key: str, sql: str, params=None, **kwargs) -> list:
    # kwargs absorbs unexpected arguments (e.g. limit=) from callers
    """
    Runs a query on the given datasource and returns rows as list of dicts.
    Works for Redshift, Postgres, MySQL, Snowflake.
    BigQuery uses its own client.query() path.
    """
    cfg = _REGISTRY.get(key, {})
    db_type = cfg.get("type", "redshift")

    if db_type == "bigquery":
        return _execute_bigquery(key, sql)

    conn = get_source_connection(key)
    try:
        if db_type == "mysql":
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return list(cur.fetchall())
        else:
            import psycopg2.extras
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def init_datasources():
    """No-op — kept for backward compatibility with main.py import."""
    pass


def _execute_bigquery(key: str, sql: str) -> list:
    client = get_source_connection(key)
    query_job = client.query(sql)
    return [dict(row) for row in query_job.result()]


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/api/datasources")
def list_datasources():
    """List all registered datasources (no credentials)."""
    result = {}
    for key, cfg in _REGISTRY.items():
        result[key] = {
            "type":    cfg.get("type"),
            "host":    cfg.get("host") or cfg.get("project") or cfg.get("account"),
            "dbname":  cfg.get("dbname") or cfg.get("database") or cfg.get("dataset"),
            "user":    cfg.get("user", ""),
            "status":  "registered",
        }
    # Always include default even if not in registry
    if "default" not in result:
        result["default"] = {
            "type":   "redshift",
            "host":   os.getenv("REDSHIFT_HOST", ""),
            "dbname": os.getenv("REDSHIFT_DB", ""),
            "user":   os.getenv("REDSHIFT_USER", ""),
            "status": "env",
        }
    return {"datasources": result, "count": len(result)}


@router.post("/api/datasources/test")
async def test_datasource(payload: dict):
    """
    Test connectivity for a registered datasource key.
    Body: { "key": "client_acme" }
    """
    key = payload.get("key", "default")
    t0 = time.time()
    try:
        cfg = _REGISTRY.get(key, {})
        db_type = cfg.get("type", "redshift")

        if db_type == "bigquery":
            client = get_source_connection(key)
            list(client.list_datasets(max_results=1))
        else:
            conn = get_source_connection(key)
            conn.close()

        ms = int((time.time() - t0) * 1000)
        return {"key": key, "status": "ok", "latency_ms": ms}
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        return JSONResponse(
            status_code=400,
            content={"key": key, "status": "error", "error": str(e), "latency_ms": ms}
        )


@router.post("/api/datasources/{key}/query")
async def query_datasource(key: str, payload: dict):
    """
    Run a SQL query on a specific datasource.
    Body: { "sql": "SELECT ..." }
    Returns up to 500 rows.
    """
    sql = payload.get("sql", "").strip()
    if not sql:
        return JSONResponse(status_code=400, content={"error": "sql is required"})

    # Safety: read-only guard
    first_word = sql.split()[0].upper()
    if first_word not in ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"):
        return JSONResponse(
            status_code=400,
            content={"error": f"Only SELECT queries allowed via this endpoint (got {first_word})"}
        )

    try:
        rows = execute_on_source(key, sql)
        return {"key": key, "rows": rows[:500], "count": len(rows)}
    except Exception as e:
        logger.error(f"[datasources] query error on '{key}': {e}")
        return JSONResponse(status_code=500, content={"key": key, "error": str(e)})


@router.get("/api/datasources/{key}/schema")
async def get_schema(key: str):
    """List tables available in a datasource."""
    try:
        cfg = _REGISTRY.get(key, {})
        db_type = cfg.get("type", "redshift")

        if db_type == "bigquery":
            client = get_source_connection(key)
            project = cfg.get("project")
            dataset = cfg.get("dataset")
            tables = [t.table_id for t in client.list_tables(f"{project}.{dataset}")]
            return {"key": key, "type": db_type, "tables": tables}

        elif db_type == "mysql":
            rows = execute_on_source(key, "SHOW TABLES")
            tables = [list(r.values())[0] for r in rows]
            return {"key": key, "type": db_type, "tables": tables}

        elif db_type == "snowflake":
            rows = execute_on_source(key, "SHOW TABLES")
            tables = [r.get("name") for r in rows]
            return {"key": key, "type": db_type, "tables": tables}

        else:
            # Redshift / Postgres
            rows = execute_on_source(
                key,
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
                "ORDER BY table_schema, table_name LIMIT 200"
            )
            return {"key": key, "type": db_type, "tables": rows}

    except Exception as e:
        return JSONResponse(status_code=500, content={"key": key, "error": str(e)})

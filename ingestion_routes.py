"""
WiziAgent — /api/ingestion/status
Executes configurable Redshift queries sent from the frontend (wz_ingestion_cfg_v1).
Add to your FastAPI app: app.include_router(ingestion_router)
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg2, psycopg2.extras
from datetime import datetime, timezone
import pytz
import math

ingestion_router = APIRouter()

# ── Redshift connection ───────────────────────────────────────────────────────

def get_redshift_conn():
    """Use the same connection as main.py — imports at call time to avoid circular import."""
    from main import get_connection
    return get_connection()


def run_query(conn, sql: str) -> list[dict]:
    """Execute a SELECT and return rows as list of dicts (psycopg2)."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    return rows


# ── Request / Response models ─────────────────────────────────────────────────

class StageQueries(BaseModel):
    metrics:  Optional[str] = None
    accounts: Optional[str] = None
    issues:   Optional[str] = None
    history:  Optional[str] = None

class QueriesPayload(BaseModel):
    downloader: Optional[StageQueries] = None
    copy:       Optional[StageQueries] = None
    quality:    Optional[StageQueries] = None

class StatusRequest(BaseModel):
    queries: Optional[QueriesPayload] = None


# ── Helpers ───────────────────────────────────────────────────────────────────

IST = pytz.timezone("Asia/Kolkata")

def fmt_time(ts) -> str:
    if ts is None: return "—"
    if isinstance(ts, str): return ts
    return ts.astimezone(IST).strftime("Today %H:%M IST")

def fmt_minutes(mins) -> str:
    if mins is None: return "—"
    m = int(mins); return f"{m}m" if m < 60 else f"{m//60}h {m%60}m"

def derive_status(metrics: dict, thresholds: dict) -> str:
    """error > warning > healthy based on metric values vs thresholds."""
    result = "healthy"
    for metric, vals in thresholds.items():
        v = metrics.get(metric)
        if v is None: continue
        if v >= vals.get("error", math.inf): return "error"
        if v >= vals.get("warn", math.inf): result = "warning"
    return result

DEFAULT_THRESHOLDS = {
    "downloader": {"failed_downloads": {"warn": 1, "error": 3}, "data_lag_hrs": {"warn": 2, "error": 6}},
    "copy":       {"replication_lag":  {"warn": 10,"error": 30},"failed_tables":{"warn": 1, "error": 3}},
    "quality":    {"checks_failed":    {"warn": 1, "error": 5}, "anomalies":    {"warn": 1, "error": 4}},
}


# ── Stage builder ─────────────────────────────────────────────────────────────

def build_downloader(conn, q: StageQueries) -> dict:
    # 1. Metrics
    m_rows = run_query(conn, q.metrics) if q.metrics else []
    m = m_rows[0] if m_rows else {}

    accounts_synced = int(m.get("accounts_synced") or 0)
    total_accounts  = int(m.get("total_accounts")  or 0)
    failed_dl       = int(m.get("failed_downloads") or 0)
    data_lag        = int(m.get("data_lag_hrs")     or 0)
    avg_dl_min      = float(m.get("avg_dl_time_min") or 0)
    last_run_ts     = m.get("last_run_ts")

    metrics_out = [
        {"id":"accounts_synced",  "label":"Accounts Synced",   "value":f"{accounts_synced} / {total_accounts}", "numeric":accounts_synced, "ok": failed_dl==0},
        {"id":"failed_downloads", "label":"Failed Downloads",  "value":str(failed_dl),  "numeric":failed_dl,  "ok": failed_dl==0},
        {"id":"avg_dl_time",      "label":"Avg Download Time", "value":fmt_minutes(avg_dl_min), "numeric":round(avg_dl_min,1), "ok":True},
        {"id":"data_lag",         "label":"Data Lag (hrs)",    "value":f"{data_lag}h",  "numeric":data_lag,   "ok": data_lag<2},
    ]

    # 2. Accounts
    acc_rows = run_query(conn, q.accounts) if q.accounts else []
    accounts_out = [
        {
            "id":   str(r.get("account","") or ""),
            "name": str(r.get("account","") or ""),
            "status": str(r.get("status","healthy")),
            "lastSync": fmt_time(r.get("last_sync")),
            "rows": int(r.get("total_rows") or 0),
        }
        for r in acc_rows
    ]

    # 3. Issues
    issue_rows = run_query(conn, q.issues) if q.issues else []
    issues_out = [
        f"{r.get('account','?')}: {r.get('report_type','?')} failed — {r.get('error_message','unknown error')}"
        for r in issue_rows
    ]

    # 4. History (7-day sparkline)
    hist_rows = run_query(conn, q.history) if q.history else []
    history_out = [int(r.get("accounts_synced") or r.get("val") or 0) for r in hist_rows]

    # 5. Status
    status = derive_status(
        {"failed_downloads": failed_dl, "data_lag_hrs": data_lag},
        DEFAULT_THRESHOLDS["downloader"]
    )
    summary = (
        f"{accounts_synced} / {total_accounts} accounts synced"
        if not issues_out else
        f"{len(issues_out)} issue(s) — {accounts_synced} / {total_accounts} synced"
    )

    return {
        "status":   status,
        "summary":  summary,
        "lastRun":  fmt_time(last_run_ts),
        "metrics":  metrics_out,
        "issues":   issues_out,
        "history":  history_out,
        "accounts": accounts_out,
        "sla":      {"expected": "05:00 IST", "actual": fmt_time(last_run_ts), "met": data_lag < 2},
        "duration": int(avg_dl_min * 60),
    }


def build_generic_stage(conn, q: StageQueries, stage_id: str) -> dict:
    """
    Generic builder for copy/quality until you wire real queries.
    Returns placeholder structure that won't break the UI.
    """
    m_rows = run_query(conn, q.metrics) if q.metrics else []
    m = m_rows[0] if m_rows else {}

    issue_rows = run_query(conn, q.issues) if q.issues else []
    issues_out = [str(list(r.values())[0]) for r in issue_rows if any(r.values())]

    hist_rows  = run_query(conn, q.history) if q.history else []
    history_out = [int(r.get("val") or list(r.values())[0] or 0) for r in hist_rows]

    acc_rows = run_query(conn, q.accounts) if q.accounts else []
    accounts_out = [
        {
            "id":   str(r.get("account","") or ""),
            "name": str(r.get("account","") or ""),
            "status": str(r.get("status","healthy")),
            "lastSync": fmt_time(r.get("last_sync")),
            "rows": int(r.get("total_rows") or 0),
        }
        for r in acc_rows
    ]

    # derive numeric metrics from whatever columns come back
    metrics_out = [
        {"id": k, "label": k.replace("_"," ").title(), "value": str(v), "numeric": float(v) if isinstance(v,(int,float)) else 0, "ok": True}
        for k, v in m.items() if v is not None
    ]

    status = "error" if issues_out else ("warning" if not m_rows else "healthy")

    return {
        "status":   status,
        "summary":  f"{len(issues_out)} issue(s)" if issues_out else "All checks passed",
        "lastRun":  fmt_time(datetime.now(timezone.utc)),
        "metrics":  metrics_out,
        "issues":   issues_out,
        "history":  history_out,
        "accounts": accounts_out,
        "sla":      {"expected": "06:00 IST", "actual": "—", "met": not issues_out},
        "duration": 0,
    }


# ── Route ─────────────────────────────────────────────────────────────────────

@ingestion_router.post("/api/ingestion/status")
async def ingestion_status(body: StatusRequest):
    """
    Accepts configurable queries from the frontend (wz_ingestion_cfg_v1.queries)
    and runs them against Redshift, returning the standard ingestion status shape.
    """
    q = body.queries or QueriesPayload()

    try:
        conn = get_redshift_conn()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redshift connection failed: {e}")

    try:
        result = {"run_id": datetime.now(timezone.utc).isoformat()}

        # Downloader
        dq = q.downloader or StageQueries()
        try:
            result["downloader"] = build_downloader(conn, dq)
        except Exception as e:
            result["downloader"] = {"status":"error","summary":f"Query error: {e}","metrics":[],"issues":[str(e)],"history":[],"accounts":[]}

        # Copy
        cq = q.copy or StageQueries()
        try:
            result["copy"] = build_generic_stage(conn, cq, "copy")
        except Exception as e:
            result["copy"] = {"status":"error","summary":f"Query error: {e}","metrics":[],"issues":[str(e)],"history":[],"accounts":[]}

        # Quality
        qq = q.quality or StageQueries()
        try:
            result["quality"] = build_generic_stage(conn, qq, "quality")
        except Exception as e:
            result["quality"] = {"status":"error","summary":f"Query error: {e}","metrics":[],"issues":[str(e)],"history":[],"accounts":[]}

        return result

    finally:
        conn.close()

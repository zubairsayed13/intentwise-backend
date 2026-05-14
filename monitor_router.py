"""
WiziAgent — Data Observatory backend
File: routers/monitor_router.py

Mount in main.py:
    from routers.monitor_router import router as monitor_router, agent_log
    app.include_router(monitor_router)
"""

from __future__ import annotations

import re
import asyncio
from collections import deque
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Literal, Optional

from fastapi import APIRouter
from pydantic import BaseModel

from secrets import get_secret          # your existing boto3 wrapper
import redshift_connector

router = APIRouter(prefix="/api/monitor", tags=["monitor"])


# ─────────────────────────────────────────────────────────────────────────────
# Redshift helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_conn():
    url = get_secret("wiziagent/redshift_url")
    m = re.match(r"redshift://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)", url)
    if not m:
        raise ValueError("Invalid redshift_url format")
    user, password, host, port, database = m.groups()
    return redshift_connector.connect(
        host=host, port=int(port), database=database,
        user=user, password=password, timeout=15,
    )


def _run(sql: str) -> list[dict]:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _scalar(sql: str, default=None):
    try:
        rows = _run(sql)
        if rows:
            return list(rows[0].values())[0]
    except Exception:
        pass
    return default


# ─────────────────────────────────────────────────────────────────────────────
# Schema introspection — cached per process lifetime
# ─────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=None)
def _get_columns(schema: str, table: str) -> set[str]:
    """Return the real lowercase column names for schema.table from information_schema."""
    rows = _run(f"""
        SELECT LOWER(column_name) AS col
        FROM information_schema.columns
        WHERE LOWER(table_schema) = LOWER('{schema}')
          AND LOWER(table_name)   = LOWER('{table}')
    """)
    return {r["col"] for r in rows}


def _has(cols: set[str], *candidates: str) -> Optional[str]:
    """Return the first candidate that actually exists in cols, else None."""
    for c in candidates:
        if c.lower() in cols:
            return c
    return None


# ─────────────────────────────────────────────────────────────────────────────
# In-memory agent event log
# ─────────────────────────────────────────────────────────────────────────────

class AgentEvent(BaseModel):
    type: str
    message: str
    table: Optional[str]       = None
    agent_name: Optional[str]  = None
    status: Optional[Literal["success", "fail", "error", "running"]] = None
    detail: Optional[str]      = None
    sql_preview: Optional[str] = None
    timestamp: Optional[str]   = None

    def model_post_init(self, __context: Any) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


class AgentEventLog:
    def __init__(self, maxlen: int = 500):
        self._q: deque[dict] = deque(maxlen=maxlen)
        self._lock = asyncio.Lock()

    def push(self, event: AgentEvent) -> None:
        self._q.appendleft(event.model_dump())

    async def apush(self, event: AgentEvent) -> None:
        async with self._lock:
            self._q.appendleft(event.model_dump())

    def recent(self, n: int = 200) -> list[dict]:
        return list(self._q)[:n]


agent_log = AgentEventLog()


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic query builders — SQL constructed entirely from discovered columns
# ─────────────────────────────────────────────────────────────────────────────

def _build_report_stats(cols: set[str]) -> dict:
    stats: dict[str, Any] = {}

    # ── Find date column ──────────────────────────────────────────────────
    date_col = _has(cols,
        "report_date", "date", "day", "reporting_date",
        "data_date", "event_date", "created_date", "period_date"
    )
    if not date_col:
        stats["report_error"] = "No date column found in mws.report"
        return stats

    # ── Find metric columns for null-rate (skip obvious non-metrics) ──────
    skip = {"id", "account_id", "account", "marketplace_id", "marketplace",
            "seller_id", "asin", "sku", "parent_asin", date_col.lower()}
    metric_cols = [c for c in sorted(cols)
                   if c not in skip and not c.endswith("_id")][:5]

    # ── Find natural-key columns for dedup ────────────────────────────────
    asin_col        = _has(cols, "asin", "parent_asin", "sku")
    marketplace_col = _has(cols, "marketplace_id", "marketplace", "country_code", "region")

    # ── Build null-rate expression ────────────────────────────────────────
    if metric_cols:
        null_cases = " + ".join(
            f"CASE WHEN {c} IS NULL THEN 1.0 ELSE 0.0 END" for c in metric_cols
        )
        null_expr = f"ROUND(({null_cases}) / {len(metric_cols)}.0 * 100, 2)"
    else:
        null_expr = "0.0"

    # ── Main stats query ──────────────────────────────────────────────────
    try:
        rows = _run(f"""
            SELECT
                COUNT(*)                                             AS total_rows,
                MAX({date_col})                                      AS latest_date,
                DATEDIFF('hour', MAX({date_col}), GETDATE())         AS hours_since_update,
                {null_expr}                                          AS null_rate_pct
            FROM mws.report
            WHERE {date_col} >= DATEADD('day', -1, GETDATE())
        """)
        if rows:
            r = rows[0]
            h = float(r.get("hours_since_update") or 0)
            stats.update({
                "report_rows":               int(r.get("total_rows") or 0),
                "report_latest_date":        str(r.get("latest_date") or ""),
                "report_hours_since_update": h,
                "null_rate":                 round(float(r.get("null_rate_pct") or 0), 2),
                "report_freshness":          f"{h:.0f}h ago" if h < 48 else f"{h/24:.1f}d ago",
                "report_date_col":           date_col,
                "report_metric_cols":        metric_cols,
            })
    except Exception as e:
        stats["report_error"] = str(e)
        return stats

    # ── 7-day rolling average ─────────────────────────────────────────────
    try:
        avg = _scalar(f"""
            SELECT AVG(daily_rows) FROM (
                SELECT {date_col}, COUNT(*) AS daily_rows
                FROM mws.report
                WHERE {date_col} >= DATEADD('day', -8, GETDATE())
                  AND {date_col} <  DATEADD('day', -1, GETDATE())
                GROUP BY {date_col}
            ) t
        """)
        if avg:
            stats["report_rows_7d_avg"] = float(avg)
    except Exception:
        pass

    # ── Deduplication check ───────────────────────────────────────────────
    key_parts = [c for c in [date_col, asin_col, marketplace_col] if c]
    if len(key_parts) > 1:
        key_cols = ", ".join(key_parts)
        try:
            dups = _scalar(f"""
                SELECT COUNT(*) FROM (
                    SELECT {key_cols}, COUNT(*) AS cnt
                    FROM mws.report
                    WHERE {date_col} >= DATEADD('day', -7, GETDATE())
                    GROUP BY {key_cols}
                    HAVING COUNT(*) > 1
                ) t
            """)
            stats["report_duplicate_count"] = int(dups or 0)
            stats["report_dedup_key"]        = key_cols
        except Exception:
            stats["report_duplicate_count"] = 0
    else:
        stats["report_duplicate_count"] = 0

    return stats


def _build_sp_api_stats(cols: set[str]) -> dict:
    stats: dict[str, Any] = {}

    # ── Find timestamp column ─────────────────────────────────────────────
    ts_col = _has(cols,
        "created_at", "request_time", "timestamp", "event_time",
        "request_date", "ingested_at", "updated_at", "date"
    )
    if not ts_col:
        stats["sp_api_error"] = "No timestamp column found in mws.sp_api_requests"
        return stats

    # ── Find status/error signal ──────────────────────────────────────────
    status_col = _has(cols, "status_code", "http_status", "response_code", "status")
    error_col  = _has(cols, "error", "error_type", "error_code", "is_error", "failed")

    if status_col:
        error_expr    = f"SUM(CASE WHEN {status_col} >= 400 THEN 1 ELSE 0 END)"
        throttle_expr = f"SUM(CASE WHEN {status_col} = 429  THEN 1 ELSE 0 END)"
    elif error_col:
        error_expr    = f"SUM(CASE WHEN {error_col} IS NOT NULL AND CAST({error_col} AS VARCHAR) != '' THEN 1 ELSE 0 END)"
        throttle_expr = "0"
    else:
        error_expr    = "0"
        throttle_expr = "0"

    # ── Main stats query ──────────────────────────────────────────────────
    try:
        rows = _run(f"""
            SELECT
                COUNT(*)                                                  AS total_count,
                {error_expr}                                              AS error_count,
                ROUND(100.0 * {error_expr} / NULLIF(COUNT(*), 0), 2)     AS error_rate_pct,
                {throttle_expr}                                           AS throttle_count
            FROM mws.sp_api_requests
            WHERE {ts_col} >= DATEADD('day', -1, GETDATE())
        """)
        if rows:
            r = rows[0]
            stats.update({
                "sp_api_count":         int(r.get("total_count") or 0),
                "sp_api_error_count":   int(r.get("error_count") or 0),
                "sp_api_error_rate":    round(float(r.get("error_rate_pct") or 0), 2),
                "sp_api_throttle_count":int(r.get("throttle_count") or 0),
                "sp_api_today":         int(r.get("total_count") or 0),
                "sp_api_ts_col":        ts_col,
                "sp_api_status_col":    status_col,
            })
    except Exception as e:
        stats["sp_api_error"] = str(e)
        return stats

    # ── 7-day average ─────────────────────────────────────────────────────
    try:
        avg7 = _scalar(f"""
            SELECT AVG(daily_cnt) FROM (
                SELECT DATE_TRUNC('day', {ts_col}) AS d, COUNT(*) AS daily_cnt
                FROM mws.sp_api_requests
                WHERE {ts_col} >= DATEADD('day', -8, GETDATE())
                  AND {ts_col} <  DATEADD('day', -1, GETDATE())
                GROUP BY 1
            ) t
        """)
        if avg7:
            stats["sp_api_7d_avg"] = float(avg7)
    except Exception:
        pass

    # ── Top erroring endpoints (only if endpoint column exists) ───────────
    endpoint_col = _has(cols, "endpoint", "api_path", "path", "operation", "api_name", "resource")
    if endpoint_col and status_col:
        try:
            stats["sp_api_top_errors"] = _run(f"""
                SELECT {endpoint_col} AS endpoint, COUNT(*) AS cnt
                FROM mws.sp_api_requests
                WHERE {ts_col} >= DATEADD('day', -1, GETDATE())
                  AND {status_col} >= 400
                GROUP BY {endpoint_col}
                ORDER BY cnt DESC
                LIMIT 3
            """)
        except Exception:
            pass

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Insight rules — derived entirely from stats dict, no hardcoded column names
# ─────────────────────────────────────────────────────────────────────────────

def _build_insights(stats: dict) -> list[dict]:
    items: list[dict] = []

    report_cols = stats.get("report_metric_cols", [])
    report_date = stats.get("report_date_col")
    sp_ts       = stats.get("sp_api_ts_col")
    sp_status   = stats.get("sp_api_status_col")

    # Schema discovery callout
    if report_cols:
        items.append({
            "title":  f"Monitoring {len(report_cols)} metric column(s) in mws.report",
            "detail": f"Columns: {', '.join(report_cols[:5])}. Date key: {report_date}.",
            "metric": f"{len(report_cols)} cols",
            "severity": "info",
        })

    # Freshness
    hours_stale = stats.get("report_hours_since_update")
    if hours_stale is not None:
        if hours_stale > 48:
            items.append({"title": "mws.report has not been updated in over 2 days",
                "detail": f"Last update was {hours_stale:.0f}h ago — check your ingestion pipeline.",
                "metric": f"{hours_stale:.0f}h since last update", "severity": "critical"})
        elif hours_stale > 24:
            items.append({"title": "mws.report update is overdue",
                "detail": f"Last update {hours_stale:.0f}h ago. Expected cadence is daily.",
                "metric": f"{hours_stale:.0f}h since last update", "severity": "high"})
        else:
            items.append({"title": "mws.report is up to date",
                "detail": f"Last updated {hours_stale:.1f}h ago.",
                "metric": f"{hours_stale:.1f}h ago", "severity": "info"})

    # Row count anomaly vs 7d avg
    report_rows = stats.get("report_rows", 0)
    avg_7d      = stats.get("report_rows_7d_avg")
    if avg_7d and avg_7d > 0:
        delta_pct = ((report_rows - avg_7d) / avg_7d) * 100
        if abs(delta_pct) > 30:
            direction = "increased" if delta_pct > 0 else "dropped"
            items.append({"title": f"mws.report row count {direction} sharply vs 7-day average",
                "detail": f"Today: {report_rows:,}  vs  7d avg: {avg_7d:,.0f}",
                "metric": f"{delta_pct:+.1f}% vs 7d avg",
                "severity": "critical" if abs(delta_pct) > 60 else "high"})
        else:
            items.append({"title": "mws.report row count is within normal range",
                "detail": f"Today: {report_rows:,}  ·  7d avg: {avg_7d:,.0f}  ({delta_pct:+.1f}%)",
                "metric": f"{delta_pct:+.1f}% vs 7d avg", "severity": "info"})

    # Null rate
    null_rate = stats.get("null_rate")
    if null_rate is not None and report_cols:
        if null_rate > 15:
            items.append({"title": "High null rate in mws.report metric columns",
                "detail": f"{null_rate:.1f}% null across: {', '.join(report_cols)}.",
                "metric": f"{null_rate:.1f}% null rate", "severity": "high"})
        elif null_rate > 5:
            items.append({"title": "Elevated null rate in mws.report",
                "detail": f"{null_rate:.1f}% of metric column values are null.",
                "metric": f"{null_rate:.1f}% null rate", "severity": "warning"})

    # Duplicates
    dups      = stats.get("report_duplicate_count", 0)
    dedup_key = stats.get("report_dedup_key", "")
    if dups > 0:
        items.append({"title": f"{dups:,} duplicate rows detected in mws.report",
            "detail": f"Duplicate key: ({dedup_key}). Deduplication may be needed.",
            "metric": f"{dups:,} duplicates",
            "severity": "high" if dups > 1000 else "warning"})

    # SP API error rate
    error_rate = stats.get("sp_api_error_rate")
    sp_note    = f" (via {sp_status})" if sp_status else ""
    if error_rate is not None:
        if error_rate > 10:
            items.append({"title": "SP API error rate is critically high",
                "detail": f"{error_rate:.1f}% of SP API requests are returning errors{sp_note}.",
                "metric": f"{error_rate:.1f}% error rate", "severity": "critical"})
        elif error_rate > 3:
            items.append({"title": "SP API error rate is elevated",
                "detail": f"{error_rate:.1f}% error rate{sp_note}. Normal is < 1%.",
                "metric": f"{error_rate:.1f}% error rate", "severity": "high"})
        else:
            items.append({"title": "SP API error rate is healthy",
                "detail": f"{error_rate:.2f}% error rate{sp_note}.",
                "metric": f"{error_rate:.2f}% error rate", "severity": "info"})

    # SP API throttling
    throttle   = stats.get("sp_api_throttle_count", 0)
    sp_total   = stats.get("sp_api_count", 0)
    if throttle > 0 and sp_total > 0:
        throttle_pct = round(throttle / sp_total * 100, 1)
        items.append({"title": f"SP API throttling detected — {throttle:,} 429 responses",
            "detail": f"{throttle_pct}% of requests throttled in the last 24h. Consider request pacing.",
            "metric": f"{throttle:,} throttled ({throttle_pct}%)",
            "severity": "high" if throttle_pct > 5 else "warning"})

    # SP API volume spike/drop
    sp_today = stats.get("sp_api_today")
    sp_7d    = stats.get("sp_api_7d_avg")
    if sp_today is not None and sp_7d and sp_7d > 0:
        delta = ((sp_today - sp_7d) / sp_7d) * 100
        if delta > 100:
            items.append({"title": f"SP API request volume spiked {delta:.0f}% above normal",
                "detail": f"Today: {sp_today:,}  vs  7d avg: {sp_7d:,.0f}. Could indicate runaway polling.",
                "metric": f"{delta:+.0f}% vs 7d avg", "severity": "warning"})
        elif delta < -50:
            items.append({"title": "SP API request volume dropped significantly",
                "detail": f"Today: {sp_today:,}  vs  7d avg: {sp_7d:,.0f}. Ingestion may have stalled.",
                "metric": f"{delta:.0f}% vs 7d avg", "severity": "high"})

    # Top erroring endpoints
    for e in stats.get("sp_api_top_errors", [])[:2]:
        items.append({"title": f"Endpoint '{e.get('endpoint','?')}' has {e.get('cnt',0):,} errors",
            "detail": "Top erroring endpoint in the last 24h.",
            "metric": f"{e.get('cnt',0):,} errors", "severity": "warning"})

    # Query errors surface first
    if "report_error" in stats:
        items.insert(0, {"title": "Could not query mws.report",
            "detail": stats["report_error"], "metric": "query error", "severity": "critical"})
    if "sp_api_error" in stats:
        items.insert(0, {"title": "Could not query mws.sp_api_requests",
            "detail": stats["sp_api_error"], "metric": "query error", "severity": "critical"})

    order = {"critical": 0, "high": 1, "warning": 2, "medium": 3, "low": 4, "info": 5}
    items.sort(key=lambda x: order.get(x["severity"], 9))
    return items


def _compute_score(stats: dict) -> int:
    score = 100
    h     = stats.get("report_hours_since_update", 0)
    if h > 48:   score -= 30
    elif h > 24: score -= 15
    n = stats.get("null_rate", 0)
    if n > 15: score -= 20
    elif n > 5: score -= 10
    er = stats.get("sp_api_error_rate", 0)
    if er > 10: score -= 25
    elif er > 3: score -= 10
    dups = stats.get("report_duplicate_count", 0)
    if dups > 1000: score -= 15
    elif dups > 0:  score -= 5
    sp_today = stats.get("sp_api_today", 0)
    sp_7d    = stats.get("sp_api_7d_avg", 0)
    if sp_7d > 0:
        d = abs((sp_today - sp_7d) / sp_7d)
        if d > 0.6:   score -= 15
        elif d > 0.3: score -= 8
    thr = stats.get("sp_api_throttle_count", 0)
    tot = stats.get("sp_api_count", 1)
    if tot > 0 and thr / tot > 0.05: score -= 10
    if "report_error"  in stats: score -= 20
    if "sp_api_error"  in stats: score -= 10
    return max(0, min(100, score))


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@router.get("/health-scorecard")
async def health_scorecard():
    """Introspects real columns via information_schema, then builds all SQL dynamically."""
    stats: dict[str, Any] = {}

    try:
        report_cols = _get_columns("mws", "report")
        stats["report_schema_cols"] = sorted(report_cols)
    except Exception as e:
        report_cols = set()
        stats["report_error"] = f"Schema introspection failed: {e}"

    try:
        sp_cols = _get_columns("mws", "sp_api_requests")
        stats["sp_api_schema_cols"] = sorted(sp_cols)
    except Exception as e:
        sp_cols = set()
        stats["sp_api_error"] = f"Schema introspection failed: {e}"

    if report_cols:
        stats.update(_build_report_stats(report_cols))
    if sp_cols:
        stats.update(_build_sp_api_stats(sp_cols))

    stats["score"]         = _compute_score(stats)
    stats["anomaly_score"] = max(0, 100 - stats["score"])
    stats["fetched_at"]    = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    return stats


class InsightsRequest(BaseModel):
    tables: list[str] = ["mws.report", "mws.sp_api_requests"]


@router.post("/ai-insights")
async def ai_insights(req: InsightsRequest):
    scorecard = await health_scorecard()
    return {
        "items":              _build_insights(scorecard),
        "generated_at":       scorecard.get("fetched_at"),
        "report_cols_found":  scorecard.get("report_schema_cols", []),
        "sp_api_cols_found":  scorecard.get("sp_api_schema_cols", []),
    }


@router.get("/agent-activity")
async def agent_activity(limit: int = 200):
    return {"events": agent_log.recent(min(limit, 500))}


@router.post("/agent-activity/push")
async def push_agent_event(event: AgentEvent):
    await agent_log.apush(event)
    return {"ok": True}


@router.get("/alerts")
async def get_alerts():
    scorecard = await health_scorecard()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return {
        "alerts": [
            {
                "title":        ins["title"],
                "severity":     ins["severity"],
                "message":      ins["detail"],
                "metric":       ins.get("metric"),
                "table":        "mws.sp_api_requests" if "sp api" in ins["title"].lower() else "mws.report",
                "count":        _extract_int(ins.get("metric", "")),
                "triggered_at": now,
            }
            for ins in _build_insights(scorecard)
            if ins["severity"] != "info"
        ]
    }


@router.get("/schema-debug")
async def schema_debug():
    """Hit this endpoint first to verify column discovery for both tables."""
    result = {}
    for schema, table in [("mws", "report"), ("mws", "sp_api_requests")]:
        key = f"{schema}.{table}"
        try:
            cols = _get_columns(schema, table)
            result[key] = {
                "total_columns":  len(cols),
                "all_columns":    sorted(cols),
                "date_col":       _has(cols, "report_date", "date", "day", "reporting_date", "data_date", "event_date", "created_date", "period_date"),
                "timestamp_col":  _has(cols, "created_at", "request_time", "timestamp", "event_time", "request_date", "ingested_at", "updated_at"),
                "status_col":     _has(cols, "status_code", "http_status", "response_code", "status"),
                "asin_col":       _has(cols, "asin", "parent_asin", "sku"),
                "marketplace_col":_has(cols, "marketplace_id", "marketplace", "country_code", "region"),
                "endpoint_col":   _has(cols, "endpoint", "api_path", "path", "operation", "api_name", "resource"),
            }
        except Exception as e:
            result[key] = {"error": str(e)}
    return result


def _extract_int(metric: str) -> Optional[int]:
    m = re.search(r"([\d,]+)", metric)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None

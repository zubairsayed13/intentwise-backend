"""
slack_integration.py — Slack AI summaries + two-way Q&A for WiziAgent
Drop next to main.py, then add:
    from slack_integration import router as slack_router
    app.include_router(slack_router)

Required env vars:
    SLACK_BOT_TOKEN   — xoxb-... (from your Slack app)
    SLACK_CHANNEL_ID  — C... (channel to post digests to)
    SLACK_SIGNING_SECRET — from Slack app settings (for request verification)
    WIZIAGENT_URL     — public URL of your dashboard e.g. https://wiziagent.vercel.app
"""

import os, json, hmac, hashlib, time, asyncio
from typing import Optional, List
from datetime import datetime, date

import httpx
from fastapi import APIRouter, Request, BackgroundTasks, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/slack", tags=["slack"])

# ── Config from env ─────────────────────────────────────────────────────────────
BOT_TOKEN      = lambda: os.getenv("SLACK_BOT_TOKEN", "")
CHANNEL_ID     = lambda: os.getenv("SLACK_CHANNEL_ID", "")
SIGNING_SECRET = lambda: os.getenv("SLACK_SIGNING_SECRET", "")
DASHBOARD_URL  = lambda: os.getenv("WIZIAGENT_URL", "https://your-dashboard.vercel.app")
OPENAI_KEY     = lambda: os.getenv("OPENAI_API_KEY", "")

# ── Slack API helpers ───────────────────────────────────────────────────────────
async def slack_post(channel: str, blocks: list, text: str = "WiziAgent update"):
    """Post a block-kit message to Slack."""
    token = BOT_TOKEN()
    if not token:
        return {"ok": False, "error": "no_token"}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"channel": channel, "text": text, "blocks": blocks}
        )
        return r.json()

async def slack_reply(channel: str, thread_ts: str, text: str):
    """Reply in a Slack thread."""
    token = BOT_TOKEN()
    if not token:
        return
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"channel": channel, "thread_ts": thread_ts, "text": text}
        )

def _verify_slack_signature(body: bytes, timestamp: str, signature: str) -> bool:
    """Verify the request came from Slack."""
    secret = SIGNING_SECRET()
    if not secret:
        return True  # skip verification in dev
    base = f"v0:{timestamp}:{body.decode()}"
    expected = "v0=" + hmac.new(secret.encode(), base.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

# ── Block builders ──────────────────────────────────────────────────────────────
def _run_summary_blocks(run: dict, ai_diagnosis: str) -> list:
    status     = run.get("status", "unknown")
    wf_name    = run.get("workflow_name", "Workflow")
    failed     = run.get("failed", 0)
    total      = run.get("total", 0)
    started_at = (run.get("started_at") or "")[:16].replace("T", " ")
    run_id     = run.get("run_id", "")

    status_emoji = "✅" if status == "clean" else "🔴"
    status_text  = "All checks passed" if status == "clean" else f"{failed}/{total} checks failed"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{status_emoji} {wf_name}"}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Status:*\n{status_text}"},
                {"type": "mrkdwn", "text": f"*Run time:*\n{started_at}"},
            ]
        }
    ]

    if ai_diagnosis and status != "clean":
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*🤖 AI Diagnosis:*\n{ai_diagnosis}"}
        })

    if status != "clean":
        failed_checks = run.get("check_results", [])
        failed_names  = [c["name"] for c in failed_checks if not c.get("passed")][:5]
        if failed_names:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Failed checks:*\n" + "\n".join(f"• {n}" for n in failed_names)}
            })

    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "🔍 View in Dashboard"},
                "url": f"{DASHBOARD_URL()}#results/{run_id}",
                "style": "primary" if status == "clean" else "danger"
            }
        ]
    })

    blocks.append({"type": "divider"})
    return blocks

def _digest_blocks(summary: dict) -> list:
    today        = date.today().strftime("%B %d, %Y")
    total_runs   = summary.get("total_runs", 0)
    failed_runs  = summary.get("failed_runs", 0)
    clean_runs   = total_runs - failed_runs
    health_pct   = int((clean_runs / total_runs * 100)) if total_runs else 0
    health_emoji = "🟢" if health_pct >= 80 else "🟡" if health_pct >= 50 else "🔴"
    narrative    = summary.get("narrative", "")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊 WiziAgent Daily Digest — {today}"}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Health:*\n{health_emoji} {health_pct}%"},
                {"type": "mrkdwn", "text": f"*Runs:*\n{clean_runs} clean / {failed_runs} failed"},
            ]
        }
    ]

    if narrative:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*🤖 AI Summary:*\n{narrative}"}
        })

    if summary.get("top_failures"):
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Top issues today:*\n" +
                "\n".join(f"• {f}" for f in summary["top_failures"][:5])}
        })

    blocks += [
        {
            "type": "actions",
            "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": "Open Dashboard"},
                 "url": DASHBOARD_URL(), "style": "primary"}
            ]
        },
        {"type": "divider"}
    ]
    return blocks

# ── AI helpers ──────────────────────────────────────────────────────────────────
async def _ai_diagnose_run(run: dict) -> str:
    """One-sentence AI diagnosis of a failed run. Falls back to rule-based."""
    failed = [c for c in run.get("check_results", []) if not c.get("passed")]
    if not failed:
        return ""

    # Rule-based zero-token fallback
    names = [c["name"] for c in failed]
    if any("ads" in n.lower() or "available" in n.lower() for n in names):
        return "Amazon Ads data has not arrived for today — downstream campaigns may be affected."
    if any("copy" in n.lower() or "gds" in n.lower() for n in names):
        return "GDS copy jobs are stuck — BI dashboards may be showing stale data."
    if any("fresh" in n.lower() or "date" in n.lower() for n in names):
        return "Data freshness issue detected — source pipeline may not have run."

    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return f"{len(failed)} check(s) failed: {', '.join(names[:3])}"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content":
                        f"Workflow '{run.get('workflow_name')}' failed. Checks: {', '.join(names)}. "
                        f"Write ONE sentence explaining the likely root cause for a business stakeholder. Max 25 words."}],
                    "max_tokens": 60, "temperature": 0.2
                }
            )
            return r.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return f"{len(failed)} check(s) failed: {', '.join(names[:3])}"

async def _ai_digest_narrative(runs: list) -> str:
    """Short narrative for the daily digest."""
    failed_runs = [r for r in runs if r.get("status") != "clean"]
    if not failed_runs:
        return "All workflows ran cleanly today. No action required."

    all_failures = []
    for r in failed_runs:
        for c in r.get("check_results", []):
            if not c.get("passed"):
                all_failures.append(f"{r.get('workflow_name')}: {c['name']}")

    key = os.getenv("OPENAI_API_KEY", "")
    if not key or not all_failures:
        return f"{len(failed_runs)} workflow(s) had failures today. Check the dashboard for details."

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content":
                        f"Today's data quality failures: {'; '.join(all_failures[:10])}. "
                        f"Write a 2-sentence business summary. What failed and what's the likely impact?"}],
                    "max_tokens": 80, "temperature": 0.2
                }
            )
            return r.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return f"{len(failed_runs)} workflow(s) had failures today."

async def _ai_answer_question(question: str, db_conn=None) -> str:
    """
    Answer a Slack Q&A question about data quality.
    Tries to pull live data from Redshift if db_conn provided, else uses AI knowledge.
    """
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return "AI Q&A is not configured (missing OPENAI_API_KEY)."

    # Try to get recent run context for grounding
    context = ""
    if db_conn:
        try:
            cur = db_conn.cursor()
            cur.execute("""
                SELECT workflow_name, status, failed, started_at
                FROM wizi_workflow_runs
                ORDER BY started_at DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
            context = "Recent runs:\n" + "\n".join(
                f"- {r[0]}: {r[1]} ({r[2]} failed) at {str(r[3])[:16]}"
                for r in rows
            )
        except Exception:
            pass

    system = """You are WiziAgent, an AI data quality assistant for an Amazon Seller analytics platform.
You monitor Redshift data pipelines, Amazon Ads data availability, and workflow check results.
Answer questions concisely (2-3 sentences). If you don't know something specific, say so and suggest checking the dashboard."""

    user_msg = f"{context}\n\nQuestion: {question}" if context else question

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user",   "content": user_msg}
                    ],
                    "max_tokens": 150, "temperature": 0.3
                }
            )
            return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"Sorry, I couldn't process that right now. Check the dashboard directly: {DASHBOARD_URL()}"

# ── Outbound: notify after a workflow run ──────────────────────────────────────
class RunNotifyRequest(BaseModel):
    run: dict
    channel_id: Optional[str] = None   # override default channel

@router.post("/notify-run")
async def notify_run(req: RunNotifyRequest, bg: BackgroundTasks):
    """
    Call this from your workflow run handler after any failed run completes.
    In main.py, after saving a run result:
        if run_data.get("failed", 0) > 0:
            await notify_run(RunNotifyRequest(run=run_data))
    """
    async def _send():
        diagnosis = await _ai_diagnose_run(req.run)
        blocks    = _run_summary_blocks(req.run, diagnosis)
        channel   = req.channel_id or CHANNEL_ID()
        if channel:
            await slack_post(channel, blocks,
                text=f"{'✅' if req.run.get('status')=='clean' else '🔴'} {req.run.get('workflow_name','Workflow')} run complete")

    bg.add_task(_send)
    return {"status": "queued"}

# ── Outbound: daily digest ──────────────────────────────────────────────────────
@router.post("/send-digest")
async def send_digest(bg: BackgroundTasks):
    """
    Call from your existing daily cron or from the Configure tab's Send Digest button.
    Fetches today's runs from DB and posts a summary to Slack.
    """
    async def _send():
        # Pull today's runs from main.py in-memory history
        try:
            from main import _wf_run_history
            today = datetime.datetime.utcnow().date().isoformat()
            runs  = [r for r in _wf_run_history
                     if (r.get("started_at") or "")[:10] == today]
        except Exception:
            runs = []

        top_failures = []
        for r in runs:
            for c in r.get("check_results", []):
                if not c.get("passed"):
                    top_failures.append(f"{r['workflow_name']}: {c['name']}")

        narrative = await _ai_digest_narrative(runs)
        summary   = {
            "total_runs":   len(runs),
            "failed_runs":  sum(1 for r in runs if r.get("status") != "clean"),
            "narrative":    narrative,
            "top_failures": top_failures[:5]
        }
        blocks  = _digest_blocks(summary)
        channel = CHANNEL_ID()
        if channel:
            await slack_post(channel, blocks, text="📊 WiziAgent Daily Digest")

    bg.add_task(_send)
    return {"status": "queued"}

# ── Inbound: Slack Events API webhook ──────────────────────────────────────────
@router.post("/events")
async def slack_events(request: Request, bg: BackgroundTasks):
    """
    Slack Events API webhook endpoint.
    Set this as your Request URL in Slack app settings:
        https://your-vm-domain.com/api/slack/events

    Required Slack app scopes:
        Bot token:  chat:write, app_mentions:read, channels:history
        Event subscriptions: app_mention
    """
    body      = await request.body()
    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")

    # Replay attack protection
    if abs(time.time() - float(timestamp or 0)) > 300:
        raise HTTPException(status_code=400, detail="Request too old")

    if not _verify_slack_signature(body, timestamp, signature):
        raise HTTPException(status_code=403, detail="Invalid signature")

    payload = json.loads(body)

    # Slack URL verification challenge (one-time on setup)
    if payload.get("type") == "url_verification":
        return {"challenge": payload["challenge"]}

    event = payload.get("event", {})

    # Only respond to app_mention events
    if event.get("type") != "app_mention":
        return {"ok": True}

    # Ignore bot's own messages
    if event.get("bot_id"):
        return {"ok": True}

    channel    = event.get("channel", "")
    thread_ts  = event.get("thread_ts") or event.get("ts", "")
    # Strip the bot mention from the message text
    text = event.get("text", "")
    import re
    question = re.sub(r"<@[A-Z0-9]+>", "", text).strip()

    if not question:
        question = "What's the current status of our data pipelines?"

    async def _respond():
        # Show typing indicator
        await slack_reply(channel, thread_ts, "⏳ Checking...")
        answer = await _ai_answer_question(question)
        await slack_reply(channel, thread_ts, answer)

    bg.add_task(_respond)
    return {"ok": True}

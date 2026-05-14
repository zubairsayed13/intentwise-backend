"""
slack_socket.py
───────────────
Handles Slack interactive button callbacks via Socket Mode.
No public URL needed — connects outbound to Slack via WebSocket.

Run alongside FastAPI:
  Start automatically when main.py starts (see bottom of main.py)

Button actions handled:
  acknowledge  → marks alert as acknowledged
  resolve      → marks alert as resolved
  escalate     → marks alert as escalated, sends follow-up message
  view         → no action needed (URL button, Slack handles it)
"""

import os, logging, threading
from slack_bolt import App
from slack_bolt.adapter.socket_mode.aiohttp import AsyncSocketModeHandler
from slack_bolt.async_app import AsyncApp

logger = logging.getLogger(__name__)

# ── In-memory alert store (replace with DB later) ─────────────────────────────
# Structure: { alert_id: { status, title, acknowledged_by, resolved_by } }
_alert_store: dict = {}

def get_alert_store() -> dict:
    return _alert_store

def update_alert(alert_id: str, status: str, user: str = ""):
    if alert_id not in _alert_store:
        _alert_store[alert_id] = {}
    _alert_store[alert_id]["status"] = status
    _alert_store[alert_id][f"{status}_by"] = user
    logger.info(f"[slack_socket] Alert {alert_id} → {status} by {user}")


# ── Slack Bolt app ─────────────────────────────────────────────────────────────
def create_slack_app():
    bot_token = os.getenv("SLACK_BOT_TOKEN", "")
    if not bot_token:
        logger.warning("[slack_socket] SLACK_BOT_TOKEN not set — skipping Slack app")
        return None
    return AsyncApp(token=bot_token)


def register_actions(slack_app):
    """Register all button action handlers (async — Windows safe)."""

    @slack_app.action("acknowledge")
    async def handle_acknowledge(ack, body, client):
        await ack()
        alert_id = body["actions"][0]["value"]
        user     = body["user"]["name"]
        update_alert(alert_id, "acknowledged", user)
        try:
            await client.chat_update(
                channel=body["channel"]["id"], ts=body["message"]["ts"],
                text=f"Acknowledged by {user}",
                blocks=[
                    {"type":"section","text":{"type":"mrkdwn","text":f"checkmark *Acknowledged* by {user}"}},
                    {"type":"context","elements":[{"type":"mrkdwn","text":f"Alert: `{alert_id}`"}]}
                ]
            )
        except Exception as e:
            logger.warning(f"[slack_socket] chat_update failed: {e}")

    @slack_app.action("resolve")
    async def handle_resolve(ack, body, client):
        await ack()
        alert_id = body["actions"][0]["value"]
        user     = body["user"]["name"]
        update_alert(alert_id, "resolved", user)
        try:
            await client.chat_update(
                channel=body["channel"]["id"], ts=body["message"]["ts"],
                text=f"Resolved by {user}",
                blocks=[
                    {"type":"section","text":{"type":"mrkdwn","text":f"checkmark *Resolved* by {user}"}},
                    {"type":"context","elements":[{"type":"mrkdwn","text":f"Alert: `{alert_id}`"}]}
                ]
            )
        except Exception as e:
            logger.warning(f"[slack_socket] chat_update failed: {e}")

    @slack_app.action("escalate")
    async def handle_escalate(ack, body, client):
        await ack()
        alert_id = body["actions"][0]["value"]
        user     = body["user"]["name"]
        update_alert(alert_id, "escalated", user)
        try:
            await client.chat_update(
                channel=body["channel"]["id"], ts=body["message"]["ts"],
                text=f"Escalated by {user}",
                blocks=[
                    {"type":"section","text":{"type":"mrkdwn","text":f"escalate *Escalated* by {user}"}},
                    {"type":"context","elements":[{"type":"mrkdwn","text":f"Alert: `{alert_id}`"}]}
                ]
            )
            channel = os.getenv("SLACK_CHANNEL_ID", body["channel"]["id"])
            await client.chat_postMessage(
                channel=channel,
                text=f"Escalation triggered by {user} for alert {alert_id}. Immediate attention required."
            )
        except Exception as e:
            logger.warning(f"[slack_socket] escalate failed: {e}")

    @slack_app.action("view")
    async def handle_view(ack):
        await ack()


async def start_socket_mode_async():
    """Async Socket Mode — Windows safe. Call from FastAPI lifespan."""
    app_token = os.getenv("SLACK_APP_TOKEN", "")
    bot_token = os.getenv("SLACK_BOT_TOKEN", "")
    if not app_token or not bot_token:
        logger.warning("[slack_socket] Tokens not set — Socket Mode disabled")
        return None
    try:
        slack_app = create_slack_app()
        if not slack_app:
            return None
        register_actions(slack_app)
        handler = AsyncSocketModeHandler(slack_app, app_token)
        await handler.connect_async()
        logger.info("[slack_socket] Socket Mode started")
        return handler
    except Exception as e:
        logger.error(f"[slack_socket] Failed: {e}")
        return None


def start_socket_mode():
    pass  # kept for import compat

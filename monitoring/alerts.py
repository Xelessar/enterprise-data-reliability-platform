"""Alert delivery. Currently Slack (Incoming Webhook); add email/PagerDuty
here later using the same send_slack_alert(message) call shape so the DAG
doesn't need to change when a channel is added.
"""
from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)


def send_slack_alert(message: str, webhook_url: str | None) -> bool:
    """Posts `message` to a Slack Incoming Webhook. Returns False (and logs)
    instead of raising, so a broken webhook never fails the pipeline itself."""
    if not webhook_url:
        logger.info("SLACK_WEBHOOK_URL not set — skipping Slack alert. Message was:\n%s", message)
        return False
    try:
        resp = requests.post(webhook_url, json={"text": message}, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error("Failed to send Slack alert: %s", exc)
        return False

"""Discord webhook alert delivery."""

from __future__ import annotations

import httpx

from arb.models.opportunity import Opportunity


class DiscordAlertError(RuntimeError):
    """Raised when a Discord webhook alert cannot be delivered."""


def format_discord_opportunity(opportunity: Opportunity) -> str:
    """Format a Discord-friendly multi-line opportunity alert."""

    return (
        f"**Arb Alert: {opportunity.team} {opportunity.side.upper()}**\n"
        f"**Event:** {opportunity.event_label}\n"
        f"**Sportsbook fair:** {opportunity.fair_probability * 100:.1f}%\n"
        f"**Kalshi executable:** {opportunity.executable_price * 100:.1f}%\n"
        f"**Net edge:** {opportunity.net_edge * 100:.1f}%\n"
        f"**Confidence:** {opportunity.match_confidence * 100:.0f}%\n"
        f"**Available depth:** {opportunity.available_size:.2f}\n"
        f"**Kalshi ticker:** `{opportunity.kalshi_ticker}`\n"
        f"**Detected:** {opportunity.detected_at.isoformat()}"
    )


def send_discord_alerts(
    webhook_url: str,
    opportunities: list[Opportunity],
    username: str = "Arb Alerts",
    timeout: float = 10.0,
) -> None:
    """Send opportunity alerts to a Discord webhook.

    Each alert is sent as a separate message to avoid Discord content-size issues.
    """

    if not webhook_url.strip():
        raise ValueError("Discord webhook URL cannot be empty.")

    with httpx.Client(timeout=timeout) as client:
        for opportunity in opportunities:
            payload = {
                "username": username,
                "content": format_discord_opportunity(opportunity),
            }
            try:
                response = client.post(webhook_url, json=payload)
                response.raise_for_status()
            except httpx.HTTPError as exc:
                raise DiscordAlertError(f"Discord webhook request failed: {exc}") from exc

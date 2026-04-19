"""Tests for alert formatting and Discord payload delivery."""

from __future__ import annotations

from datetime import UTC, datetime

from arb.alerts.console import format_opportunity
from arb.alerts.discord import format_discord_opportunity, send_discord_alerts
from arb.models.opportunity import Opportunity


def build_opportunity() -> Opportunity:
    """Create a reusable opportunity fixture."""

    return Opportunity(
        sportsbook_game_id="game-1",
        event_label="North Carolina vs Duke",
        kalshi_ticker="KXNCAAB-1",
        side="yes",
        team="North Carolina",
        fair_probability=0.58,
        executable_price=0.50,
        available_size=40.0,
        raw_edge=0.08,
        net_edge=0.07,
        match_confidence=0.97,
        explanation="Matched on league, teams, and time.",
        detected_at=datetime(2026, 4, 20, 0, 0, tzinfo=UTC),
    )


def test_format_opportunity_contains_key_fields() -> None:
    message = format_opportunity(build_opportunity())

    assert "Arb Alert: North Carolina YES" in message
    assert "Event: North Carolina vs Duke" in message
    assert "Sportsbook fair: 58.0%" in message
    assert "Kalshi executable: 50.0%" in message
    assert "Net edge: 7.0%" in message
    assert "Kalshi ticker: KXNCAAB-1" in message


def test_format_discord_opportunity_contains_key_fields() -> None:
    message = format_discord_opportunity(build_opportunity())

    assert "**Arb Alert: North Carolina YES**" in message
    assert "**Event:** North Carolina vs Duke" in message
    assert "**Sportsbook fair:** 58.0%" in message
    assert "**Kalshi ticker:** `KXNCAAB-1`" in message


def test_send_discord_alerts_posts_each_opportunity(monkeypatch) -> None:
    sent_payloads: list[dict] = []

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "DummyClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def post(self, url: str, json: dict) -> DummyResponse:
            sent_payloads.append({"url": url, "json": json})
            return DummyResponse()

    monkeypatch.setattr("arb.alerts.discord.httpx.Client", DummyClient)

    send_discord_alerts(
        webhook_url="https://discord.example/webhook",
        opportunities=[build_opportunity(), build_opportunity()],
        username="Arb Alerts",
    )

    assert len(sent_payloads) == 2
    assert sent_payloads[0]["url"] == "https://discord.example/webhook"
    assert sent_payloads[0]["json"]["username"] == "Arb Alerts"
    assert "**Event:** North Carolina vs Duke" in sent_payloads[0]["json"]["content"]

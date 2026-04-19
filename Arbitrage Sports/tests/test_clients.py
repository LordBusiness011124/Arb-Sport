"""Fixture-based integration tests for external client normalization."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from arb.clients.kalshi import KalshiClient, _parse_optional_datetime
from arb.clients.sportsbook import SportsbookClient


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str) -> object:
    """Load a JSON fixture from disk."""

    return json.loads((FIXTURES_DIR / name).read_text())


def test_sportsbook_client_normalizes_realistic_odds_payload() -> None:
    client = SportsbookClient(
        base_url="https://api.the-odds-api.com",
        api_key="test",
        bookmaker="draftkings",
        sport="basketball_ncaab",
    )
    payload = load_fixture("sportsbook_odds_sample.json")
    fetched_at = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)

    assert isinstance(payload, list)
    market = client._normalize_event(payload[0], fetched_at=fetched_at)

    assert market is not None
    assert market.game.game_id == "evt-123"
    assert market.game.home_team == "North Carolina"
    assert market.game.away_team == "Duke"
    assert market.home_american_odds == -125
    assert market.away_american_odds == 105
    assert market.home_fair_probability > market.away_fair_probability
    assert market.fetched_at == fetched_at


def test_kalshi_client_normalizes_realistic_market_and_orderbook_payloads() -> None:
    events_payload = load_fixture("kalshi_events_sample.json")
    orderbook_payload = load_fixture("kalshi_orderbook_sample.json")

    class StubKalshiClient(KalshiClient):
        def _get_json(self, path: str, params: dict[str, str] | None = None) -> dict:
            if path == "/events":
                assert params is not None
                return events_payload  # type: ignore[return-value]
            if path.startswith("/markets/") and path.endswith("/orderbook"):
                return orderbook_payload  # type: ignore[return-value]
            raise AssertionError(f"Unexpected path: {path}")

    client = StubKalshiClient()
    snapshots = client.fetch_active_sports_markets(limit=10)

    assert len(snapshots) == 2

    clean_market = next(snapshot for snapshot in snapshots if snapshot.ticker == "KXNCAAB-UNCDUKE-NC")
    assert clean_market.event_title == "North Carolina vs Duke"
    assert clean_market.event_sub_title == "North Carolina vs Duke"
    assert clean_market.market_type == "binary"
    assert clean_market.yes_sub_title == "North Carolina beats Duke"
    assert clean_market.occurrence_time == datetime(2026, 4, 20, 1, 0, tzinfo=UTC)

    # Explicit top-of-book ask should override the derived first level.
    assert clean_market.yes_ask == 0.5
    assert clean_market.yes_ask_size == 25.0
    assert clean_market.no_ask == 0.54
    assert clean_market.no_ask_size == 20.0

    # Remaining depth should still be preserved for fill-adjusted pricing.
    assert len(clean_market.yes_ask_levels) >= 2
    assert clean_market.yes_ask_levels[0].price == 0.5
    assert clean_market.yes_ask_levels[1].price >= clean_market.yes_ask_levels[0].price


def test_sportsbook_client_rejects_naive_commence_time() -> None:
    client = SportsbookClient(
        base_url="https://api.the-odds-api.com",
        api_key="test",
        bookmaker="draftkings",
        sport="basketball_nba",
    )
    fetched_at = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    event = {
        "id": "evt-naive",
        "home_team": "Boston Celtics",
        "away_team": "New York Knicks",
        "commence_time": "2026-04-20T19:00:00",
        "bookmakers": [
            {
                "key": "draftkings",
                "markets": [
                    {
                        "key": "h2h",
                        "outcomes": [
                            {"name": "Boston Celtics", "price": -120},
                            {"name": "New York Knicks", "price": 110},
                        ],
                    }
                ],
            }
        ],
    }

    assert client._normalize_event(event, fetched_at=fetched_at) is None


def test_kalshi_client_rejects_naive_occurrence_datetime() -> None:
    assert _parse_optional_datetime("2026-04-20T19:00:00") is None

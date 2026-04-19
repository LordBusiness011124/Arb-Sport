"""Tests for scan safety gating in the main service loop."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import sqlite3

from arb.config import Settings
from arb.main import (
    MatchableKalshiMarket,
    _build_matchable_kalshi_market,
    _run_scan,
    _reject_stale_or_skewed_match,
    _select_unique_match,
)
from arb.models.market import KalshiEventMarket, KalshiMarketSnapshot
from arb.models.market import KalshiOrderLevel
from arb.models.odds import SportsbookGame, SportsbookMoneylineMarket
from arb.services.storage import initialize_schema


def build_settings() -> Settings:
    """Create reusable settings for service-loop safety tests."""

    return Settings(
        sportsbook_api_key="test",
        sportsbook_base_url="https://api.the-odds-api.com",
        sportsbook_name="draftkings",
        sportsbook_sport="basketball_ncaab",
        sportsbook_regions="us",
        kalshi_base_url="https://api.elections.kalshi.com/trade-api/v2",
        discord_webhook_url=None,
        discord_username="Arb Alerts",
        sportsbook_poll_seconds=180,
        kalshi_poll_seconds=45,
        edge_threshold=0.03,
        fee_rate=0.0,
        slippage=0.0,
        target_order_size=25.0,
        minimum_liquidity=25.0,
        max_sportsbook_snapshot_age_seconds=120,
        max_kalshi_snapshot_age_seconds=60,
        max_cross_feed_skew_seconds=30,
        max_match_time_diff_minutes=120,
        match_confidence_threshold=0.90,
        ambiguous_match_confidence_delta=0.02,
        max_book_dislocation=0.02,
        alert_only_yes_signals=True,
        sqlite_path=Path("data/app.db"),
        sqlite_retention_days=14,
        kalshi_market_limit=250,
        loop_sleep_seconds=1.0,
    )


def build_sportsbook_market(
    fetched_at: datetime,
    home_team: str = "North Carolina",
    away_team: str = "Duke",
    start_time: datetime | None = None,
) -> SportsbookMoneylineMarket:
    """Create a sportsbook market fixture."""

    return SportsbookMoneylineMarket(
        game=SportsbookGame(
            game_id="game-1",
            league="NCAAB",
            home_team=home_team,
            away_team=away_team,
            start_time=start_time or datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        ),
        bookmaker="draftkings",
        fetched_at=fetched_at,
        home_american_odds=-110,
        away_american_odds=-110,
        home_implied_probability=0.5238,
        away_implied_probability=0.5238,
        home_fair_probability=0.5,
        away_fair_probability=0.5,
    )


def build_kalshi_candidate(
    fetched_at: datetime,
    occurrence_time: datetime,
    yes_team: str = "UNC",
    no_team: str = "Duke",
    ticker: str = "KXNCAAB-1",
) -> MatchableKalshiMarket:
    """Create a Kalshi candidate fixture."""

    snapshot = KalshiMarketSnapshot(
        ticker=ticker,
        event_ticker=ticker,
        event_title="North Carolina vs Duke",
        event_sub_title="North Carolina vs Duke",
        market_title=f"Will {yes_team} beat {no_team}?",
        series_ticker="KXNCAAB",
        category="Sports",
        status="active",
        fetched_at=fetched_at,
        occurrence_time=occurrence_time,
        market_type="binary",
        yes_sub_title=f"{yes_team} beats {no_team}",
        no_sub_title=f"{yes_team} beats {no_team}",
        rules_primary=f"If {yes_team} wins against {no_team}, then the market resolves to Yes.",
        yes_ask_levels=(
            KalshiOrderLevel(price=0.50, size=100.0),
            KalshiOrderLevel(price=0.52, size=100.0),
        ),
        no_ask_levels=(
            KalshiOrderLevel(price=0.50, size=100.0),
            KalshiOrderLevel(price=0.52, size=100.0),
        ),
        yes_bid=0.48,
        yes_bid_size=100.0,
        yes_ask=0.50,
        yes_ask_size=100.0,
        no_bid=0.48,
        no_bid_size=100.0,
        no_ask=0.50,
        no_ask_size=100.0,
    )
    event_market = KalshiEventMarket(
        ticker=ticker,
        league="college basketball",
        team_a=yes_team,
        team_b=no_team,
        start_time=occurrence_time,
        title=snapshot.market_title,
    )
    return MatchableKalshiMarket(
        event_market=event_market,
        snapshot=snapshot,
        yes_team=yes_team,
        no_team=no_team,
    )


def test_reject_stale_or_skewed_match_rejects_stale_sportsbook_snapshot() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    settings = build_settings()
    sportsbook_market = build_sportsbook_market(fetched_at=now - timedelta(seconds=121))
    kalshi_candidate = build_kalshi_candidate(
        fetched_at=now - timedelta(seconds=5),
        occurrence_time=datetime(2026, 4, 20, 1, 30, tzinfo=UTC),
    )

    rejection = _reject_stale_or_skewed_match(
        sportsbook_market,
        kalshi_candidate.snapshot,
        settings,
        now=now,
    )

    assert rejection is not None
    assert rejection.reason == "sportsbook_snapshot_stale"


def test_reject_stale_or_skewed_match_rejects_cross_feed_skew() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    settings = build_settings()
    sportsbook_market = build_sportsbook_market(fetched_at=now - timedelta(seconds=10))
    kalshi_candidate = build_kalshi_candidate(
        fetched_at=now - timedelta(seconds=50),
        occurrence_time=datetime(2026, 4, 20, 1, 30, tzinfo=UTC),
    )

    rejection = _reject_stale_or_skewed_match(
        sportsbook_market,
        kalshi_candidate.snapshot,
        settings,
        now=now,
    )

    assert rejection is not None
    assert rejection.reason == "cross_feed_skew_too_large"


def test_select_unique_match_rejects_ambiguous_candidates() -> None:
    settings = build_settings()
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
    )
    candidates = [
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 1, 10, tzinfo=UTC),
            ticker="KXNCAAB-1",
        ),
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 1, 12, tzinfo=UTC),
            ticker="KXNCAAB-2",
        ),
    ]

    selected = _select_unique_match(
        sportsbook_market=sportsbook_market,
        candidates=candidates,
        settings=settings,
    )

    assert selected is None


def test_select_unique_match_accepts_clear_single_best_candidate() -> None:
    settings = build_settings()
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
    )
    candidates = [
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 1, 1, tzinfo=UTC),
            ticker="KXNCAAB-best",
        ),
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 2, 30, tzinfo=UTC),
            ticker="KXNCAAB-worse",
        ),
    ]

    selected = _select_unique_match(
        sportsbook_market=sportsbook_market,
        candidates=candidates,
        settings=settings,
    )

    assert selected is not None
    assert selected[0].snapshot.ticker == "KXNCAAB-best"


def test_build_matchable_kalshi_market_rejects_qualified_contract_scope() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    snapshot = KalshiMarketSnapshot(
        ticker="KXPGAH2H-1",
        event_ticker="KXPGAH2H-1",
        event_title="4th Round Head-to-Head: Fitzpatrick vs Scheffler",
        event_sub_title="4th Round - Fitzpatrick vs Scheffler",
        market_title="Will Matt Fitzpatrick beat Scottie Scheffler in the 4th round of the RBC Heritage?",
        series_ticker="KXPGAH2H",
        category="Sports",
        status="active",
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 17, 50, tzinfo=UTC),
        market_type="binary",
        yes_sub_title="Matt Fitzpatrick beats Scottie Scheffler in the 4th round",
        no_sub_title="Matt Fitzpatrick beats Scottie Scheffler in the 4th round",
        rules_primary="If Matt Fitzpatrick wins the head-to-head matchup against Scottie Scheffler in the 4th round, the market resolves to Yes.",
        yes_ask_levels=(KalshiOrderLevel(price=0.38, size=2.0),),
        no_ask_levels=(KalshiOrderLevel(price=0.63, size=10.0),),
        yes_bid=0.37,
        yes_bid_size=50.0,
        yes_ask=0.38,
        yes_ask_size=2.0,
        no_bid=0.62,
        no_bid_size=50.0,
        no_ask=0.63,
        no_ask_size=10.0,
    )

    assert _build_matchable_kalshi_market(snapshot, "basketball_ncaab") is None


def test_build_matchable_kalshi_market_accepts_clean_structured_matchup() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    snapshot = KalshiMarketSnapshot(
        ticker="KXNCAAB-structured",
        event_ticker="KXNCAAB-structured",
        event_title="North Carolina vs Duke",
        event_sub_title="North Carolina vs Duke",
        market_title="Will North Carolina beat Duke?",
        series_ticker="KXNCAAB",
        category="Sports",
        status="active",
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        market_type="binary",
        yes_sub_title="North Carolina beats Duke",
        no_sub_title="North Carolina beats Duke",
        rules_primary="If North Carolina wins against Duke, then the market resolves to Yes.",
        yes_ask_levels=(KalshiOrderLevel(price=0.50, size=100.0),),
        no_ask_levels=(KalshiOrderLevel(price=0.50, size=100.0),),
        yes_bid=0.48,
        yes_bid_size=100.0,
        yes_ask=0.50,
        yes_ask_size=100.0,
        no_bid=0.48,
        no_bid_size=100.0,
        no_ask=0.50,
        no_ask_size=100.0,
    )

    matchable = _build_matchable_kalshi_market(snapshot, "basketball_ncaab")

    assert matchable is not None
    assert matchable.yes_team == "North Carolina"
    assert matchable.no_team == "Duke"


def test_build_matchable_kalshi_market_accepts_direct_team_label_outcomes() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    snapshot = KalshiMarketSnapshot(
        ticker="KXNCAAB-direct-labels",
        event_ticker="KXNCAAB-direct-labels",
        event_title="North Carolina vs Duke",
        event_sub_title="North Carolina vs Duke",
        market_title="Who will win?",
        series_ticker="KXNCAAB",
        category="Sports",
        status="active",
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        market_type="binary",
        yes_sub_title="North Carolina",
        no_sub_title="Duke",
        rules_primary="If North Carolina wins the game, then the market resolves to Yes.",
        yes_ask_levels=(KalshiOrderLevel(price=0.50, size=100.0),),
        no_ask_levels=(KalshiOrderLevel(price=0.50, size=100.0),),
        yes_bid=0.48,
        yes_bid_size=100.0,
        yes_ask=0.50,
        yes_ask_size=100.0,
        no_bid=0.48,
        no_bid_size=100.0,
        no_ask=0.50,
        no_ask_size=100.0,
    )

    matchable = _build_matchable_kalshi_market(snapshot, "basketball_ncaab")

    assert matchable is not None
    assert matchable.yes_team == "North Carolina"
    assert matchable.no_team == "Duke"


def test_run_scan_skips_opportunity_for_crossed_book() -> None:
    connection = sqlite3.connect(":memory:")
    initialize_schema(connection)
    settings = build_settings()
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
    )
    candidate = build_kalshi_candidate(
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        yes_team="North Carolina",
        no_team="Duke",
        ticker="KXNCAAB-crossed",
    )
    candidate.snapshot.yes_bid = 0.55
    candidate.snapshot.yes_ask = 0.50

    _run_scan(connection, settings, [sportsbook_market], [candidate.snapshot])

    opportunity_count = connection.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    assert opportunity_count == 0


def test_run_scan_persists_opportunity_for_clean_book() -> None:
    connection = sqlite3.connect(":memory:")
    initialize_schema(connection)
    settings = build_settings()
    settings.edge_threshold = 0.02
    settings.target_order_size = 25.0
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        home_team="North Carolina",
        away_team="Duke",
    )
    sportsbook_market.home_fair_probability = 0.58
    sportsbook_market.away_fair_probability = 0.42
    candidate = build_kalshi_candidate(
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        yes_team="North Carolina",
        no_team="Duke",
        ticker="KXNCAAB-clean",
    )
    candidate.snapshot.yes_ask = 0.50
    candidate.snapshot.yes_ask_levels = (
        KalshiOrderLevel(price=0.50, size=25.0),
        KalshiOrderLevel(price=0.51, size=25.0),
    )
    candidate.snapshot.no_ask = 0.60
    candidate.snapshot.no_ask_levels = (
        KalshiOrderLevel(price=0.60, size=25.0),
    )

    _run_scan(connection, settings, [sportsbook_market], [candidate.snapshot])

    row = connection.execute(
        "SELECT side, team, executable_price, net_edge FROM opportunities"
    ).fetchone()
    assert row is not None
    assert row[0] == "yes"
    assert row[1] == "North Carolina"
    assert row[2] == 0.50
    assert row[3] > settings.edge_threshold
        edge_threshold=0.03,
        fee_rate=0.0,
        slippage=0.0,
        target_order_size=25.0,
        minimum_liquidity=25.0,
        max_sportsbook_snapshot_age_seconds=120,
        max_kalshi_snapshot_age_seconds=60,
        max_cross_feed_skew_seconds=30,
        max_match_time_diff_minutes=120,
        match_confidence_threshold=0.90,
        ambiguous_match_confidence_delta=0.02,
        max_book_dislocation=0.02,
        alert_only_yes_signals=True,
        sqlite_path=Path("data/app.db"),
        sqlite_retention_days=14,
        kalshi_market_limit=250,
        loop_sleep_seconds=1.0,
    )


def build_sportsbook_market(
    fetched_at: datetime,
    home_team: str = "North Carolina",
    away_team: str = "Duke",
    start_time: datetime | None = None,
) -> SportsbookMoneylineMarket:
    """Create a sportsbook market fixture."""

    return SportsbookMoneylineMarket(
        game=SportsbookGame(
            game_id="game-1",
            league="NCAAB",
            home_team=home_team,
            away_team=away_team,
            start_time=start_time or datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        ),
        bookmaker="draftkings",
        fetched_at=fetched_at,
        home_american_odds=-110,
        away_american_odds=-110,
        home_implied_probability=0.5238,
        away_implied_probability=0.5238,
        home_fair_probability=0.5,
        away_fair_probability=0.5,
    )


def build_kalshi_candidate(
    fetched_at: datetime,
    occurrence_time: datetime,
    yes_team: str = "UNC",
    no_team: str = "Duke",
    ticker: str = "KXNCAAB-1",
) -> MatchableKalshiMarket:
    """Create a Kalshi candidate fixture."""

    snapshot = KalshiMarketSnapshot(
        ticker=ticker,
        event_ticker=ticker,
        event_title="North Carolina vs Duke",
        event_sub_title="North Carolina vs Duke",
        market_title=f"Will {yes_team} beat {no_team}?",
        series_ticker="KXNCAAB",
        category="Sports",
        status="active",
        fetched_at=fetched_at,
        occurrence_time=occurrence_time,
        market_type="binary",
        yes_sub_title=f"{yes_team} beats {no_team}",
        no_sub_title=f"{yes_team} beats {no_team}",
        rules_primary=f"If {yes_team} wins against {no_team}, then the market resolves to Yes.",
        yes_ask_levels=(
            KalshiOrderLevel(price=0.50, size=100.0),
            KalshiOrderLevel(price=0.52, size=100.0),
        ),
        no_ask_levels=(
            KalshiOrderLevel(price=0.50, size=100.0),
            KalshiOrderLevel(price=0.52, size=100.0),
        ),
        yes_bid=0.48,
        yes_bid_size=100.0,
        yes_ask=0.50,
        yes_ask_size=100.0,
        no_bid=0.48,
        no_bid_size=100.0,
        no_ask=0.50,
        no_ask_size=100.0,
    )
    event_market = KalshiEventMarket(
        ticker=ticker,
        league="college basketball",
        team_a=yes_team,
        team_b=no_team,
        start_time=occurrence_time,
        title=snapshot.market_title,
    )
    return MatchableKalshiMarket(
        event_market=event_market,
        snapshot=snapshot,
        yes_team=yes_team,
        no_team=no_team,
    )


def test_reject_stale_or_skewed_match_rejects_stale_sportsbook_snapshot() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    settings = build_settings()
    sportsbook_market = build_sportsbook_market(fetched_at=now - timedelta(seconds=121))
    kalshi_candidate = build_kalshi_candidate(
        fetched_at=now - timedelta(seconds=5),
        occurrence_time=datetime(2026, 4, 20, 1, 30, tzinfo=UTC),
    )

    rejection = _reject_stale_or_skewed_match(
        sportsbook_market,
        kalshi_candidate.snapshot,
        settings,
        now=now,
    )

    assert rejection is not None
    assert rejection.reason == "sportsbook_snapshot_stale"


def test_reject_stale_or_skewed_match_rejects_cross_feed_skew() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    settings = build_settings()
    sportsbook_market = build_sportsbook_market(fetched_at=now - timedelta(seconds=10))
    kalshi_candidate = build_kalshi_candidate(
        fetched_at=now - timedelta(seconds=50),
        occurrence_time=datetime(2026, 4, 20, 1, 30, tzinfo=UTC),
    )

    rejection = _reject_stale_or_skewed_match(
        sportsbook_market,
        kalshi_candidate.snapshot,
        settings,
        now=now,
    )

    assert rejection is not None
    assert rejection.reason == "cross_feed_skew_too_large"


def test_select_unique_match_rejects_ambiguous_candidates() -> None:
    settings = build_settings()
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
    )
    candidates = [
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 1, 10, tzinfo=UTC),
            ticker="KXNCAAB-1",
        ),
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 1, 12, tzinfo=UTC),
            ticker="KXNCAAB-2",
        ),
    ]

    selected = _select_unique_match(
        sportsbook_market=sportsbook_market,
        candidates=candidates,
        settings=settings,
    )

    assert selected is None


def test_select_unique_match_accepts_clear_single_best_candidate() -> None:
    settings = build_settings()
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
    )
    candidates = [
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 1, 1, tzinfo=UTC),
            ticker="KXNCAAB-best",
        ),
        build_kalshi_candidate(
            fetched_at=now,
            occurrence_time=datetime(2026, 4, 20, 2, 30, tzinfo=UTC),
            ticker="KXNCAAB-worse",
        ),
    ]

    selected = _select_unique_match(
        sportsbook_market=sportsbook_market,
        candidates=candidates,
        settings=settings,
    )

    assert selected is not None
    assert selected[0].snapshot.ticker == "KXNCAAB-best"


def test_build_matchable_kalshi_market_rejects_qualified_contract_scope() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    snapshot = KalshiMarketSnapshot(
        ticker="KXPGAH2H-1",
        event_ticker="KXPGAH2H-1",
        event_title="4th Round Head-to-Head: Fitzpatrick vs Scheffler",
        event_sub_title="4th Round - Fitzpatrick vs Scheffler",
        market_title="Will Matt Fitzpatrick beat Scottie Scheffler in the 4th round of the RBC Heritage?",
        series_ticker="KXPGAH2H",
        category="Sports",
        status="active",
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 17, 50, tzinfo=UTC),
        market_type="binary",
        yes_sub_title="Matt Fitzpatrick beats Scottie Scheffler in the 4th round",
        no_sub_title="Matt Fitzpatrick beats Scottie Scheffler in the 4th round",
        rules_primary="If Matt Fitzpatrick wins the head-to-head matchup against Scottie Scheffler in the 4th round, the market resolves to Yes.",
        yes_ask_levels=(KalshiOrderLevel(price=0.38, size=2.0),),
        no_ask_levels=(KalshiOrderLevel(price=0.63, size=10.0),),
        yes_bid=0.37,
        yes_bid_size=50.0,
        yes_ask=0.38,
        yes_ask_size=2.0,
        no_bid=0.62,
        no_bid_size=50.0,
        no_ask=0.63,
        no_ask_size=10.0,
    )

    assert _build_matchable_kalshi_market(snapshot, "basketball_ncaab") is None


def test_build_matchable_kalshi_market_accepts_clean_structured_matchup() -> None:
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    snapshot = KalshiMarketSnapshot(
        ticker="KXNCAAB-structured",
        event_ticker="KXNCAAB-structured",
        event_title="North Carolina vs Duke",
        event_sub_title="North Carolina vs Duke",
        market_title="Will North Carolina beat Duke?",
        series_ticker="KXNCAAB",
        category="Sports",
        status="active",
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        market_type="binary",
        yes_sub_title="North Carolina beats Duke",
        no_sub_title="North Carolina beats Duke",
        rules_primary="If North Carolina wins against Duke, then the market resolves to Yes.",
        yes_ask_levels=(KalshiOrderLevel(price=0.50, size=100.0),),
        no_ask_levels=(KalshiOrderLevel(price=0.50, size=100.0),),
        yes_bid=0.48,
        yes_bid_size=100.0,
        yes_ask=0.50,
        yes_ask_size=100.0,
        no_bid=0.48,
        no_bid_size=100.0,
        no_ask=0.50,
        no_ask_size=100.0,
    )

    matchable = _build_matchable_kalshi_market(snapshot, "basketball_ncaab")

    assert matchable is not None
    assert matchable.yes_team == "North Carolina"
    assert matchable.no_team == "Duke"


def test_run_scan_skips_opportunity_for_crossed_book() -> None:
    connection = sqlite3.connect(":memory:")
    initialize_schema(connection)
    settings = build_settings()
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
    )
    candidate = build_kalshi_candidate(
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        yes_team="North Carolina",
        no_team="Duke",
        ticker="KXNCAAB-crossed",
    )
    candidate.snapshot.yes_bid = 0.55
    candidate.snapshot.yes_ask = 0.50

    _run_scan(connection, settings, [sportsbook_market], [candidate.snapshot])

    opportunity_count = connection.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
    assert opportunity_count == 0


def test_run_scan_persists_opportunity_for_clean_book() -> None:
    connection = sqlite3.connect(":memory:")
    initialize_schema(connection)
    settings = build_settings()
    settings.edge_threshold = 0.02
    settings.target_order_size = 25.0
    now = datetime(2026, 4, 20, 0, 0, tzinfo=UTC)
    sportsbook_market = build_sportsbook_market(
        fetched_at=now,
        start_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        home_team="North Carolina",
        away_team="Duke",
    )
    sportsbook_market.home_fair_probability = 0.58
    sportsbook_market.away_fair_probability = 0.42
    candidate = build_kalshi_candidate(
        fetched_at=now,
        occurrence_time=datetime(2026, 4, 20, 1, 0, tzinfo=UTC),
        yes_team="North Carolina",
        no_team="Duke",
        ticker="KXNCAAB-clean",
    )
    candidate.snapshot.yes_ask = 0.50
    candidate.snapshot.yes_ask_levels = (
        KalshiOrderLevel(price=0.50, size=25.0),
        KalshiOrderLevel(price=0.51, size=25.0),
    )
    candidate.snapshot.no_ask = 0.60
    candidate.snapshot.no_ask_levels = (
        KalshiOrderLevel(price=0.60, size=25.0),
    )

    _run_scan(connection, settings, [sportsbook_market], [candidate.snapshot])

    row = connection.execute(
        "SELECT side, team, executable_price, net_edge FROM opportunities"
    ).fetchone()
    assert row is not None
    assert row[0] == "yes"
    assert row[1] == "North Carolina"
    assert row[2] == 0.50
    assert row[3] > settings.edge_threshold

"""Environment-based runtime configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(slots=True)
class Settings:
    """Runtime settings for polling, matching, and persistence."""

    sportsbook_api_key: str
    sportsbook_base_url: str
    sportsbook_name: str
    sportsbook_sport: str
    sportsbook_regions: str
    kalshi_base_url: str
    discord_webhook_url: str | None
    discord_username: str
    sportsbook_poll_seconds: int
    kalshi_poll_seconds: int
    edge_threshold: float
    fee_rate: float
    slippage: float
    target_order_size: float
    minimum_liquidity: float
    max_sportsbook_snapshot_age_seconds: int
    max_kalshi_snapshot_age_seconds: int
    max_cross_feed_skew_seconds: int
    max_match_time_diff_minutes: int
    match_confidence_threshold: float
    ambiguous_match_confidence_delta: float
    max_book_dislocation: float
    alert_only_yes_signals: bool
    sqlite_path: Path
    sqlite_retention_days: int
    kalshi_market_limit: int
    loop_sleep_seconds: float


def load_settings() -> Settings:
    """Load settings from the environment."""

    sportsbook_api_key = os.getenv("SPORTSBOOK_API_KEY", "").strip()
    if not sportsbook_api_key:
        raise ValueError("SPORTSBOOK_API_KEY is required.")

    return Settings(
        sportsbook_api_key=sportsbook_api_key,
        sportsbook_base_url=os.getenv("SPORTSBOOK_BASE_URL", "https://api.the-odds-api.com"),
        sportsbook_name=os.getenv("SPORTSBOOK_NAME", "draftkings"),
        sportsbook_sport=os.getenv("SPORTSBOOK_SPORT", "basketball_ncaab"),
        sportsbook_regions=os.getenv("SPORTSBOOK_REGIONS", "us"),
        kalshi_base_url=os.getenv(
            "KALSHI_BASE_URL",
            "https://api.elections.kalshi.com/trade-api/v2",
        ),
        discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", "").strip() or None,
        discord_username=os.getenv("DISCORD_USERNAME", "Arb Alerts"),
        sportsbook_poll_seconds=int(os.getenv("SPORTSBOOK_POLL_SECONDS", "180")),
        kalshi_poll_seconds=int(os.getenv("KALSHI_POLL_SECONDS", "45")),
        edge_threshold=float(os.getenv("EDGE_THRESHOLD", "0.03")),
        fee_rate=float(os.getenv("FEE_RATE", "0.00")),
        slippage=float(os.getenv("SLIPPAGE", "0.00")),
        target_order_size=float(os.getenv("TARGET_ORDER_SIZE", "25")),
        minimum_liquidity=float(os.getenv("MINIMUM_LIQUIDITY", "25")),
        max_sportsbook_snapshot_age_seconds=int(
            os.getenv("MAX_SPORTSBOOK_SNAPSHOT_AGE_SECONDS", "120")
        ),
        max_kalshi_snapshot_age_seconds=int(
            os.getenv("MAX_KALSHI_SNAPSHOT_AGE_SECONDS", "60")
        ),
        max_cross_feed_skew_seconds=int(os.getenv("MAX_CROSS_FEED_SKEW_SECONDS", "30")),
        max_match_time_diff_minutes=int(os.getenv("MAX_MATCH_TIME_DIFF_MINUTES", "120")),
        match_confidence_threshold=float(os.getenv("MATCH_CONFIDENCE_THRESHOLD", "0.90")),
        ambiguous_match_confidence_delta=float(
            os.getenv("AMBIGUOUS_MATCH_CONFIDENCE_DELTA", "0.02")
        ),
        max_book_dislocation=float(os.getenv("MAX_BOOK_DISLOCATION", "0.02")),
        alert_only_yes_signals=os.getenv("ALERT_ONLY_YES_SIGNALS", "true").strip().lower()
        not in {"0", "false", "no"},
        sqlite_path=Path(os.getenv("SQLITE_PATH", "data/app.db")),
        sqlite_retention_days=int(os.getenv("SQLITE_RETENTION_DAYS", "14")),
        kalshi_market_limit=int(os.getenv("KALSHI_MARKET_LIMIT", "250")),
        loop_sleep_seconds=float(os.getenv("LOOP_SLEEP_SECONDS", "1.0")),
    )

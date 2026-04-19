"""SQLite helpers for storing scans, normalized quotes, and opportunities."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from arb.models.market import KalshiMarketSnapshot
from arb.models.odds import SportsbookMoneylineMarket
from arb.models.opportunity import Opportunity


def connect_sqlite(path: Path) -> sqlite3.Connection:
    """Connect to SQLite and ensure parent directories exist."""

    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_schema(connection: sqlite3.Connection) -> None:
    """Create the minimal tables needed by the polling service."""

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            sportsbook_market_count INTEGER NOT NULL,
            kalshi_market_count INTEGER NOT NULL,
            matched_event_count INTEGER NOT NULL,
            opportunity_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sportsbook_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            league TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            start_time TEXT NOT NULL,
            bookmaker TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            home_american_odds INTEGER NOT NULL,
            away_american_odds INTEGER NOT NULL,
            home_fair_probability REAL NOT NULL,
            away_fair_probability REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS kalshi_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            event_ticker TEXT NOT NULL,
            event_title TEXT NOT NULL,
            market_title TEXT NOT NULL,
            category TEXT NOT NULL,
            status TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            occurrence_time TEXT,
            yes_bid REAL,
            yes_bid_size REAL,
            yes_ask REAL,
            yes_ask_size REAL,
            no_bid REAL,
            no_bid_size REAL,
            no_ask REAL,
            no_ask_size REAL
        );

        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            sportsbook_game_id TEXT NOT NULL,
            event_label TEXT NOT NULL,
            kalshi_ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            team TEXT NOT NULL,
            fair_probability REAL NOT NULL,
            executable_price REAL NOT NULL,
            available_size REAL NOT NULL,
            raw_edge REAL NOT NULL,
            net_edge REAL NOT NULL,
            match_confidence REAL NOT NULL,
            explanation TEXT NOT NULL,
            detected_at TEXT NOT NULL
        );
        """
    )
    _ensure_column(connection, "sportsbook_markets", "fetched_at", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "kalshi_markets", "fetched_at", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "kalshi_markets", "occurrence_time", "TEXT")
    _ensure_column(connection, "opportunities", "event_label", "TEXT NOT NULL DEFAULT ''")
    connection.commit()


def create_scan_run(
    connection: sqlite3.Connection,
    started_at: datetime,
    sportsbook_market_count: int,
    kalshi_market_count: int,
    matched_event_count: int,
    opportunity_count: int,
) -> int:
    """Insert a scan summary row and return its id."""

    cursor = connection.execute(
        """
        INSERT INTO scan_runs (
            started_at,
            sportsbook_market_count,
            kalshi_market_count,
            matched_event_count,
            opportunity_count
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            started_at.isoformat(),
            sportsbook_market_count,
            kalshi_market_count,
            matched_event_count,
            opportunity_count,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def store_sportsbook_markets(
    connection: sqlite3.Connection,
    scan_run_id: int,
    markets: list[SportsbookMoneylineMarket],
) -> None:
    """Persist normalized sportsbook markets for one scan."""

    connection.executemany(
        """
        INSERT INTO sportsbook_markets (
            scan_run_id,
            game_id,
            league,
            home_team,
            away_team,
            start_time,
            bookmaker,
            fetched_at,
            home_american_odds,
            away_american_odds,
            home_fair_probability,
            away_fair_probability
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                scan_run_id,
                market.game.game_id,
                market.game.league,
                market.game.home_team,
                market.game.away_team,
                market.game.start_time.isoformat(),
                market.bookmaker,
                market.fetched_at.isoformat(),
                market.home_american_odds,
                market.away_american_odds,
                market.home_fair_probability,
                market.away_fair_probability,
            )
            for market in markets
        ],
    )
    connection.commit()


def store_kalshi_markets(
    connection: sqlite3.Connection,
    scan_run_id: int,
    markets: list[KalshiMarketSnapshot],
) -> None:
    """Persist normalized Kalshi order book summaries for one scan."""

    connection.executemany(
        """
        INSERT INTO kalshi_markets (
            scan_run_id,
            ticker,
            event_ticker,
            event_title,
            market_title,
            category,
            status,
            fetched_at,
            occurrence_time,
            yes_bid,
            yes_bid_size,
            yes_ask,
            yes_ask_size,
            no_bid,
            no_bid_size,
            no_ask,
            no_ask_size
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                scan_run_id,
                market.ticker,
                market.event_ticker,
                market.event_title,
                market.market_title,
                market.category,
                market.status,
                market.fetched_at.isoformat(),
                market.occurrence_time.isoformat() if market.occurrence_time else None,
                market.yes_bid,
                market.yes_bid_size,
                market.yes_ask,
                market.yes_ask_size,
                market.no_bid,
                market.no_bid_size,
                market.no_ask,
                market.no_ask_size,
            )
            for market in markets
        ],
    )
    connection.commit()


def store_opportunities(
    connection: sqlite3.Connection,
    scan_run_id: int,
    opportunities: list[Opportunity],
) -> None:
    """Persist detected opportunities for one scan."""

    connection.executemany(
        """
        INSERT INTO opportunities (
            scan_run_id,
            sportsbook_game_id,
            event_label,
            kalshi_ticker,
            side,
            team,
            fair_probability,
            executable_price,
            available_size,
            raw_edge,
            net_edge,
            match_confidence,
            explanation,
            detected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                scan_run_id,
                opportunity.sportsbook_game_id,
                opportunity.event_label,
                opportunity.kalshi_ticker,
                opportunity.side,
                opportunity.team,
                opportunity.fair_probability,
                opportunity.executable_price,
                opportunity.available_size,
                opportunity.raw_edge,
                opportunity.net_edge,
                opportunity.match_confidence,
                opportunity.explanation,
                opportunity.detected_at.isoformat(),
            )
            for opportunity in opportunities
        ],
    )
    connection.commit()


def prune_old_scan_data(
    connection: sqlite3.Connection,
    retention_days: int,
    now: datetime | None = None,
) -> int:
    """Delete old scan data so the SQLite database stays bounded.

    All scan-linked tables are pruned together to avoid orphaned rows.

    Args:
        connection: Open SQLite connection.
        retention_days: Number of days of history to keep. Must be non-negative.
        now: Optional current time override for deterministic tests.

    Returns:
        Number of scan runs removed.
    """

    if retention_days < 0:
        raise ValueError("SQLite retention days cannot be negative.")

    cutoff = (now or datetime.now(tz=UTC)) - timedelta(days=retention_days)
    cutoff_iso = cutoff.isoformat()

    stale_scan_run_ids = [
        int(row["id"] if isinstance(row, sqlite3.Row) else row[0])
        for row in connection.execute(
            "SELECT id FROM scan_runs WHERE started_at < ?",
            (cutoff_iso,),
        ).fetchall()
    ]
    if not stale_scan_run_ids:
        return 0

    placeholders = ", ".join("?" for _ in stale_scan_run_ids)
    for table_name in ("sportsbook_markets", "kalshi_markets", "opportunities"):
        connection.execute(
            f"DELETE FROM {table_name} WHERE scan_run_id IN ({placeholders})",
            stale_scan_run_ids,
        )
    connection.execute(
        f"DELETE FROM scan_runs WHERE id IN ({placeholders})",
        stale_scan_run_ids,
    )
    connection.commit()
    return len(stale_scan_run_ids)


def _ensure_column(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_definition: str,
) -> None:
    """Add a missing column so old SQLite files remain usable after schema changes."""

    existing_columns = {
        row["name"] if isinstance(row, sqlite3.Row) else row[1]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in existing_columns:
        return

    connection.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
    )

"""Tests for SQLite schema initialization, upgrades, and retention cleanup."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

from arb.services.storage import initialize_schema, prune_old_scan_data


def test_initialize_schema_adds_missing_columns_to_existing_tables() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row

    connection.executescript(
        """
        CREATE TABLE sportsbook_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            league TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            start_time TEXT NOT NULL,
            bookmaker TEXT NOT NULL,
            home_american_odds INTEGER NOT NULL,
            away_american_odds INTEGER NOT NULL,
            home_fair_probability REAL NOT NULL,
            away_fair_probability REAL NOT NULL
        );

        CREATE TABLE kalshi_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            ticker TEXT NOT NULL,
            event_ticker TEXT NOT NULL,
            event_title TEXT NOT NULL,
            market_title TEXT NOT NULL,
            category TEXT NOT NULL,
            status TEXT NOT NULL,
            yes_bid REAL,
            yes_bid_size REAL,
            yes_ask REAL,
            yes_ask_size REAL,
            no_bid REAL,
            no_bid_size REAL,
            no_ask REAL,
            no_ask_size REAL
        );

        CREATE TABLE opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_run_id INTEGER NOT NULL,
            sportsbook_game_id TEXT NOT NULL,
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

    initialize_schema(connection)

    sportsbook_columns = {
        row["name"] for row in connection.execute("PRAGMA table_info(sportsbook_markets)")
    }
    kalshi_columns = {
        row["name"] for row in connection.execute("PRAGMA table_info(kalshi_markets)")
    }
    opportunity_columns = {
        row["name"] for row in connection.execute("PRAGMA table_info(opportunities)")
    }

    assert "fetched_at" in sportsbook_columns
    assert "fetched_at" in kalshi_columns
    assert "occurrence_time" in kalshi_columns
    assert "event_label" in opportunity_columns


def test_prune_old_scan_data_removes_scan_linked_rows() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    initialize_schema(connection)

    now = datetime(2026, 4, 19, 12, 0, tzinfo=UTC)
    old_started_at = (now - timedelta(days=20)).isoformat()
    recent_started_at = (now - timedelta(days=1)).isoformat()

    old_scan_id = int(
        connection.execute(
            """
            INSERT INTO scan_runs (
                started_at,
                sportsbook_market_count,
                kalshi_market_count,
                matched_event_count,
                opportunity_count
            ) VALUES (?, 1, 1, 1, 1)
            """,
            (old_started_at,),
        ).lastrowid
    )
    recent_scan_id = int(
        connection.execute(
            """
            INSERT INTO scan_runs (
                started_at,
                sportsbook_market_count,
                kalshi_market_count,
                matched_event_count,
                opportunity_count
            ) VALUES (?, 1, 1, 1, 1)
            """,
            (recent_started_at,),
        ).lastrowid
    )

    for scan_run_id in (old_scan_id, recent_scan_id):
        connection.execute(
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
            ) VALUES (?, 'game-1', 'nba', 'Boston', 'New York', ?, 'draftkings', ?, -120, 110, 0.55, 0.45)
            """,
            (scan_run_id, now.isoformat(), now.isoformat()),
        )
        connection.execute(
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
            ) VALUES (?, 'KXNBA-1', 'EVT-1', 'Boston vs New York', 'Will Boston beat New York?', 'sports', 'open', ?, ?, 0.48, 50, 0.50, 50, 0.50, 50, 0.52, 50)
            """,
            (scan_run_id, now.isoformat(), now.isoformat()),
        )
        connection.execute(
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
            ) VALUES (?, 'game-1', 'Boston vs New York', 'KXNBA-1', 'yes', 'Boston', 0.55, 0.50, 50, 0.05, 0.05, 0.98, 'test', ?)
            """,
            (scan_run_id, now.isoformat()),
        )
    connection.commit()

    removed = prune_old_scan_data(connection, retention_days=14, now=now)

    assert removed == 1
    assert connection.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0] == 1
    assert (
        connection.execute(
            "SELECT COUNT(*) FROM sportsbook_markets WHERE scan_run_id = ?",
            (old_scan_id,),
        ).fetchone()[0]
        == 0
    )
    assert (
        connection.execute(
            "SELECT COUNT(*) FROM kalshi_markets WHERE scan_run_id = ?",
            (old_scan_id,),
        ).fetchone()[0]
        == 0
    )
    assert (
        connection.execute(
            "SELECT COUNT(*) FROM opportunities WHERE scan_run_id = ?",
            (old_scan_id,),
        ).fetchone()[0]
        == 0
    )
    assert (
        connection.execute(
            "SELECT COUNT(*) FROM opportunities WHERE scan_run_id = ?",
            (recent_scan_id,),
        ).fetchone()[0]
        == 1
    )

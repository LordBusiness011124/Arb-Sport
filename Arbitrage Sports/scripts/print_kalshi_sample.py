"""Print sample normalized Kalshi sports market data.

Usage:
    python scripts/print_kalshi_sample.py
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from arb.clients.kalshi import KalshiAPIError, KalshiClient


def main() -> int:
    """Fetch a few open sports markets and print normalized snapshots."""

    client = KalshiClient()

    try:
        markets = client.fetch_active_sports_markets(limit=5)
    except KalshiAPIError as exc:
        print(f"Kalshi fetch failed: {exc}")
        return 1

    if not markets:
        print("No active sports markets found.")
        return 0

    for market in markets:
        print(json.dumps(asdict(market), indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

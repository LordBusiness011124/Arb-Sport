"""Console-based alert formatting for detected opportunities."""

from __future__ import annotations

from arb.models.opportunity import Opportunity


def format_opportunity(opportunity: Opportunity) -> str:
    """Format a detected opportunity into a readable multi-line alert."""

    return (
        f"Arb Alert: {opportunity.team} {opportunity.side.upper()}\n"
        f"Event: {opportunity.event_label}\n"
        f"Sportsbook fair: {opportunity.fair_probability * 100:.1f}%\n"
        f"Kalshi executable: {opportunity.executable_price * 100:.1f}%\n"
        f"Net edge: {opportunity.net_edge * 100:.1f}%\n"
        f"Confidence: {opportunity.match_confidence * 100:.0f}%\n"
        f"Available depth: {opportunity.available_size:.2f}\n"
        f"Kalshi ticker: {opportunity.kalshi_ticker}\n"
        f"Detected: {opportunity.detected_at.isoformat()}"
    )

"""Telegram notification helpers."""

from __future__ import annotations

import logging
import os
from typing import List

import requests

LOGGER = logging.getLogger(__name__)


class TelegramNotifier:
    """Send formatted Telegram messages."""

    def __init__(self) -> None:
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not self.bot_token or not self.chat_id:
            raise ValueError("Missing Telegram credentials")
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_message(self, message: str) -> None:
        """Send a Telegram message."""

        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}
        try:
            response = requests.post(
                f"{self.base_url}/sendMessage", json=payload, timeout=30
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to send Telegram message: %s", exc)

    @staticmethod
    def format_signal_message(
        ticker: str,
        entry: float,
        target: float,
        stop: float,
        position_size: float,
        news: List[str],
        earnings_date: str | None,
    ) -> str:
        """Format a signal message for Telegram."""

        news_lines = "\n".join(f"- {headline}" for headline in news) or "- None"
        earnings_line = earnings_date or "Unknown"
        link = f"https://finance.yahoo.com/quote/{ticker}"
        return (
            f"<b>Signal: {ticker}</b>\n"
            f"Link: {link}\n"
            f"Entry: {entry:.2f}\n"
            f"Target (+25%): {target:.2f}\n"
            f"Hard Stop (ATR): {stop:.2f}\n"
            f"Position Size: {position_size:.2f} shares\n"
            f"Catalyst (News):\n{news_lines}\n"
            f"Next Earnings: {earnings_line}\n"
            "Risk Warning: Position size capped to 5% risk of capital."
        )

    def send_system_down(self, error: str) -> None:
        """Send a system down alert."""

        message = f"System Down: {error}"
        self.send_message(message)

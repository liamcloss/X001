"""Trading 212 API client and universe filtering."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List
from pathlib import Path

import requests

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"

load_dotenv(ENV_FILE)

def require(*names: str) -> None:
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

LOGGER = logging.getLogger(__name__)


class RetryError(Exception):
    """Raised when retry attempts are exhausted."""


def retry_with_backoff(max_attempts: int = 5, base_delay: float = 1.0) -> Any:
    """Retry a function with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts before raising.
        base_delay: Base delay in seconds for the backoff.

    Returns:
        Wrapped function output.
    """

    def decorator(func: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - required for resilience
                    if attempt >= max_attempts:
                        LOGGER.exception("API call failed after %s attempts", attempt)
                        raise RetryError(str(exc)) from exc
                    sleep_time = base_delay * (2 ** (attempt - 1))
                    LOGGER.warning(
                        "API call failed on attempt %s: %s. Retrying in %.1fs",
                        attempt,
                        exc,
                        sleep_time,
                    )
                    time.sleep(sleep_time)
                    attempt += 1

        return wrapper

    return decorator


@dataclass(frozen=True)
class Instrument:
    """Represents a filtered Trading 212 instrument."""

    ticker: str
    name: str
    exchange: str
    instrument_type: str


class Trading212Client:
    """Client for Trading 212 universe ingestion."""

    def __init__(self) -> None:
        self.base_url = "https://live.trading212.com/api/v1"
        self.api_key = os.getenv("T212_API_KEY")
        self.trading_secret = os.getenv("T212_TRADING_SECRET")
        if not self.api_key or not self.trading_secret:
            raise ValueError(
                "Missing Trading 212 API credentials. "
                "Set T212_API_KEY and T212_TRADING_SECRET in your environment."
            )

    def _headers(self) -> Dict[str, str]:
        """Build API headers for Trading 212."""

        return {
            "Authorization": f"Bearer {self.api_key}",
            "Trading-212-API-Key": self.trading_secret,
            "Accept": "application/json",
        }

    @retry_with_backoff(max_attempts=5, base_delay=1.0)
    def fetch_instruments(self) -> List[Dict[str, Any]]:
        """Fetch all instruments from Trading 212."""

        url = f"{self.base_url}/equity/metadata/instruments"
        LOGGER.info("Fetching instruments from Trading 212")
        response = requests.get(url, headers=self._headers(), timeout=30)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def isa_exchanges() -> Iterable[str]:
        """Return ISA-compliant exchanges."""

        return {"NYSE", "NASDAQ", "LSE", "XETRA"}

    def filter_instruments(self, instruments: List[Dict[str, Any]]) -> List[Instrument]:
        """Filter instruments for equities and ISA-compliant exchanges."""

        filtered: List[Instrument] = []
        allowed_exchanges = self.isa_exchanges()
        for instrument in instruments:
            if instrument.get("type") != "EQUITY":
                continue
            exchange = instrument.get("exchange")
            if exchange not in allowed_exchanges:
                continue
            ticker = instrument.get("ticker") or instrument.get("symbol")
            name = instrument.get("name", "Unknown")
            if not ticker:
                continue
            filtered.append(
                Instrument(
                    ticker=ticker,
                    name=name,
                    exchange=exchange,
                    instrument_type=instrument.get("type", "EQUITY"),
                )
            )
        LOGGER.info("Filtered %s ISA-eligible equities", len(filtered))
        return filtered

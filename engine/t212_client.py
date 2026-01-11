"""Trading 212 API client and universe filtering."""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List

import requests

def require(*names: str) -> None:
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

LOGGER = logging.getLogger(__name__)


class RetryError(Exception):
    """Raised when retry attempts are exhausted."""


class NonRetryableError(Exception):
    """Raised when retries should not be attempted."""


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
                except NonRetryableError:
                    raise
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

        credentials = f"{self.api_key}:{self.trading_secret}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode(
            "utf-8"
        )
        return {
            "Authorization": f"Basic {encoded_credentials}",
            "Accept": "application/json",
        }

    @retry_with_backoff(max_attempts=5, base_delay=1.0)
    def fetch_instruments(self) -> List[Dict[str, Any]]:
        """Fetch all instruments from Trading 212."""

        url = f"{self.base_url}/equity/metadata/instruments"
        LOGGER.info("Fetching instruments from Trading 212")
        response = requests.get(url, headers=self._headers(), timeout=30)
        if response.status_code == 401:
            raise NonRetryableError(
                "Trading 212 API unauthorized. "
                "Check T212_API_KEY and T212_TRADING_SECRET."
            )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def isa_exchanges() -> Iterable[str]:
        """Return ISA-compliant exchanges."""

        return {"NYSE", "NASDAQ", "LSE", "XETRA"}

    @staticmethod
    def working_schedule_exchange_map() -> Dict[str, str]:
        """Map Trading 212 working schedule IDs to exchange labels."""

        return {
            "US_EQUITY": "NYSE/NASDAQ",
            "LSE_EQUITY": "LSE",
            "XETRA_EQUITY": "XETRA",
        }

    def filter_instruments(self, instruments: List[Dict[str, Any]]) -> List[Instrument]:
        """Filter instruments for equities and ISA-compliant exchanges."""

        filtered: List[Instrument] = []
        schedule_map = self.working_schedule_exchange_map()
        for instrument in instruments:
            if instrument.get("type") != "EQUITY":
                continue
            schedule_id = instrument.get("workingScheduleId")
            exchange = schedule_map.get(schedule_id)
            if not exchange:
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

    def _cache_valid(self, cache_path: Path, max_age_days: int) -> bool:
        if not cache_path.exists():
            return False
        modified_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - modified_time < timedelta(days=max_age_days)

    def _load_cached_universe(self, cache_path: Path) -> List[Instrument]:
        with cache_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return [
            Instrument(
                ticker=item["ticker"],
                name=item["name"],
                exchange=item["exchange"],
                instrument_type=item.get("instrument_type", "EQUITY"),
            )
            for item in payload
        ]

    def _save_cached_universe(self, cache_path: Path, instruments: List[Instrument]) -> None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "ticker": inst.ticker,
                "name": inst.name,
                "exchange": inst.exchange,
                "instrument_type": inst.instrument_type,
            }
            for inst in instruments
        ]
        with cache_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    def get_universe(
        self,
        cache_path: str = "data/universe.json",
        max_age_days: int = 7,
    ) -> List[Instrument]:
        """Return the cached universe, refreshing from the API when stale."""

        cache_file = Path(cache_path)
        if self._cache_valid(cache_file, max_age_days):
            LOGGER.info("Loading cached universe from %s", cache_file)
            return self._load_cached_universe(cache_file)

        LOGGER.info("Refreshing universe cache from Trading 212")
        raw_instruments = self.fetch_instruments()
        filtered = self.filter_instruments(raw_instruments)
        self._save_cached_universe(cache_file, filtered)
        return filtered

"""Main orchestrator for Trading 212 ISA universe ingestion and scanner."""

from __future__ import annotations

import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from dotenv import load_dotenv

from engine.notifier import TelegramNotifier
from engine.scanner import Scanner
from engine.t212_client import Instrument, Trading212Client

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "trading_universe.db"
CRASH_LOG = BASE_DIR / "crash_report.log"

LOGGER = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure logging for the application."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(BASE_DIR / "scanner.log"),
        ],
    )


def init_db() -> None:
    """Initialize the SQLite database schema."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe (
                ticker TEXT PRIMARY KEY,
                name TEXT,
                exchange TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                ticker TEXT,
                signal_date TEXT,
                UNIQUE(ticker, signal_date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blacklist (
                ticker TEXT PRIMARY KEY,
                reason TEXT,
                updated_at TEXT
            )
            """
        )
        conn.commit()


def upsert_universe(instruments: Iterable[Instrument]) -> None:
    """Insert or update the trading universe."""

    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT INTO universe (ticker, name, exchange, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                name=excluded.name,
                exchange=excluded.exchange,
                updated_at=excluded.updated_at
            """,
            [(ins.ticker, ins.name, ins.exchange, now) for ins in instruments],
        )
        conn.commit()


def fetch_blacklist() -> List[str]:
    """Return blacklisted tickers."""

    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT ticker FROM blacklist").fetchall()
    return [row[0] for row in rows]


def already_signaled(ticker: str, signal_date: str) -> bool:
    """Check if a signal has already been recorded."""

    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT 1 FROM signals WHERE ticker=? AND signal_date=?",
            (ticker, signal_date),
        ).fetchone()
    return row is not None


def record_signal(ticker: str, signal_date: str) -> None:
    """Record a signal in history."""

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO signals (ticker, signal_date) VALUES (?, ?)",
            (ticker, signal_date),
        )
        conn.commit()


def run_pipeline() -> None:
    """Run the universe ingestion and alpha scanner pipeline."""

    load_dotenv()
    setup_logging()
    init_db()

    t212 = Trading212Client()
    notifier = TelegramNotifier()

    instruments = t212.filter_instruments(t212.fetch_instruments())
    upsert_universe(instruments)

    blacklist = set(fetch_blacklist())
    tickers = [ins.ticker for ins in instruments if ins.ticker not in blacklist]

    scanner = Scanner(capital=1000.0)
    signals = scanner.scan(tickers)

    today = datetime.now(timezone.utc).date().isoformat()
    for signal in signals:
        if already_signaled(signal.ticker, today):
            LOGGER.info("Skipping duplicate signal for %s", signal.ticker)
            continue
        message = notifier.format_signal_message(
            ticker=signal.ticker,
            entry=signal.entry,
            target=signal.target,
            stop=signal.stop,
            position_size=signal.position_size,
            news=signal.news,
            earnings_date=signal.earnings_date,
        )
        notifier.send_message(message)
        record_signal(signal.ticker, today)


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as exc:  # noqa: BLE001 - global safety net
        CRASH_LOG.write_text(
            f"{datetime.now(timezone.utc).isoformat()} - Fatal error: {exc}\n",
            encoding="utf-8",
        )
        try:
            load_dotenv()
            notifier = TelegramNotifier()
            notifier.send_system_down(str(exc))
        except Exception:  # noqa: BLE001
            pass
        raise

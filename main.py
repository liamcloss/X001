import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env"
load_dotenv(ENV_FILE)

from engine.t212_client import Trading212Client
from engine.scanner import AlphaScanner
from engine.persistence import DatabaseManager
from engine.notifier import Notifier

# --- ADD THIS BLOCK TO THE TOP OF main.py ---
# Ensure necessary directories exist
for folder in ['logs', 'data']:
    if not os.path.exists(folder):
        os.makedirs(folder)
# --------------------------------------------

# Now your logging config will work without crashing
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

def main():
    try:
        t212 = Trading212Client()
        scanner = AlphaScanner()
        db = DatabaseManager()
        bot = Notifier()

        # 1. Cache Check + Ingest
        isa_universe = t212.get_universe()

        logging.info("Preparing scan universe for %s stocks...", len(isa_universe))

        tickers = []
        for inst in isa_universe:
            yf_ticker = scanner.clean_ticker(inst.ticker)
            if db.is_blacklisted(yf_ticker) or db.was_alerted_recently(yf_ticker):
                continue
            tickers.append(yf_ticker)

        unique_tickers = sorted(set(tickers))
        logging.info("Starting batch scan for %s stocks...", len(unique_tickers))

        # 2. Batch Scan
        signals = scanner.scan_universe(unique_tickers)
        for signal in signals:
            bot.send_alert(signal)
            db.record_signal(signal["ticker"], signal["entry"])

    except Exception:
        logging.exception("FATAL ERROR")
        # Send one-off Telegram error if needed
        raise

if __name__ == "__main__":
    main()

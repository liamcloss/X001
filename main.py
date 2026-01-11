import logging, os
from engine.t212_client import Trading212Client
from engine.scanner import AlphaScanner
from engine.persistence import DatabaseManager
from engine.notifier import Notifier
import time

# --- ADD THIS BLOCK TO THE TOP OF main.py ---
# Ensure necessary directories exist
for folder in ['logs', 'data']:
    if not os.path.exists(folder):
        os.makedirs(folder)
# --------------------------------------------

# Now your logging config will work without crashing
logging.basicConfig(
    level=logging.INFO,
    filename="logs/pipeline.log",
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    try:
        t212 = Trading212Client()
        scanner = AlphaScanner()
        db = DatabaseManager()
        bot = Notifier()

        # 1. Ingest
        raw_instruments = t212.fetch_instruments()
        isa_universe = t212.filter_instruments(raw_instruments)

        logging.info(f"Starting scan for {len(isa_universe)} stocks...")

        for inst in isa_universe:
            yf_ticker = scanner.clean_ticker(inst.ticker)
            
            if db.is_blacklisted(yf_ticker) or db.was_alerted_recently(yf_ticker):
                continue

            # 2. Scan (with rate limiting)
            signal = scanner.scan_ticker(yf_ticker)
            if signal:
                bot.send_alert(signal)
                db.record_signal(yf_ticker, signal['entry'])
            
            time.sleep(2) # Protect against YFinance IP block

    except Exception as e:
        logging.error(f"FATAL ERROR: {e}")
        # Send one-off Telegram error if needed

if __name__ == "__main__":
    main()
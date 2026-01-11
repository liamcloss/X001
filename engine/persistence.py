import sqlite3
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path="data/trading_universe.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    ticker TEXT, date TEXT, price REAL, PRIMARY KEY(ticker, date)
                )""")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS blacklist (
                    ticker TEXT, expiry_date TEXT PRIMARY KEY
                )""")

    def is_blacklisted(self, ticker):
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute("SELECT 1 FROM blacklist WHERE ticker = ? AND expiry_date > ?",
                               (ticker, datetime.now().isoformat())).fetchone()
            return res is not None

    def was_alerted_recently(self, ticker, days=7):
        since = (datetime.now() - timedelta(days=days)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute("SELECT 1 FROM signals WHERE ticker = ? AND date > ?",
                               (ticker, since)).fetchone()
            return res is not None

    def record_signal(self, ticker, price):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO signals VALUES (?, ?, ?)",
                         (ticker, datetime.now().isoformat(), price))

    def was_alerted_recently(self, ticker, days=21):
        """
        Checks if we've already signaled this ticker in the last 3 weeks.
        3 weeks (21 days) matches your 3-4 week swing trade window.
        """
        # If we alerted on this ticker recently, we don't want to see it again
        # until the typical swing trade duration has passed.
        since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute(
                "SELECT 1 FROM signals WHERE ticker = ? AND date > ?",
                (ticker, since)
            ).fetchone()
            return res is not None
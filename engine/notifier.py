import requests
import os

class Notifier:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

    def send_alert(self, data):
        msg = (
            f"🚀 *SWING SIGNAL: {data['ticker']}*\n"
            f"💰 Entry: `{data['entry']}`\n"
            f"🎯 Target (+25%): `{data['target']}`\n"
            f"🛑 Stop (2x ATR): `{data['stop']}`\n"
            f"📊 RSI: {data['rsi']}\n\n"
            f"📰 *Catalysts:*\n{data['news']}\n\n"
            f"🔗 [Yahoo Finance](https://finance.yahoo.com/quote/{data['ticker']})"
        )
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        requests.post(url, json={"chat_id": self.chat_id, "text": msg, "parse_mode": "Markdown"})
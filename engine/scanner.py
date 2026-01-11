import logging
import time

import numpy as np
import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)

class AlphaScanner:
    def __init__(self, target_upside=0.25):
        self.target_upside = target_upside

    def clean_ticker(self, t212_ticker: str) -> str:
        """Maps T212 ticker format to YFinance format."""
        # Example: AAPL_US_EQ -> AAPL | VOD_LSE_EQ -> VOD.L
        if t212_ticker.endswith((".L", ".DE")):
            return t212_ticker
        parts = t212_ticker.split("_")
        symbol = parts[0]
        if len(parts) > 1:
            market = parts[1]
            if market == "LSE":
                return f"{symbol}.L"
            if market == "XETRA":
                return f"{symbol}.DE"
        if "LSE" in t212_ticker:
            return f"{symbol}.L"
        if "XETRA" in t212_ticker:
            return f"{symbol}.DE"
        return symbol

    def calculate_atr(self, df, window=14):
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        return true_range.rolling(window=window).mean()

    def _scan_dataframe(self, ticker, df):
        try:
            if len(df) < 200:
                return None

            # 1. Liquidity Filter (> £500k avg volume)
            avg_vol_value = (df['Close'] * df['Volume']).tail(20).mean()
            if avg_vol_value < 500000:
                return None

            # 2. Strategy: Momentum Igniter
            # Price > 200 SMA, RSI crossing 50, Vol > 2x Avg
            sma200 = df['Close'].rolling(200).mean().iloc[-1]
            current_price = df['Close'].iloc[-1]
            if current_price < sma200:
                return None

            # RSI Logic
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]

            vol_avg = df['Volume'].rolling(20).mean().iloc[-1]
            current_vol = df['Volume'].iloc[-1]

            if current_price > sma200 and rsi > 50 and current_vol > (vol_avg * 2):
                atr = self.calculate_atr(df).iloc[-1]
                
                # Risk Setup
                stop_loss = current_price - (2 * atr)
                target = current_price * (1 + self.target_upside)
                
                # News & Catalyst
                info = yf.Ticker(ticker)
                news = info.news[:2]
                news_text = "\n".join([f"• {n['title']}" for n in news])

                return {
                    "ticker": ticker,
                    "entry": round(current_price, 2),
                    "stop": round(stop_loss, 2),
                    "target": round(target, 2),
                    "rsi": round(rsi, 1),
                    "news": news_text
                }
        except Exception:
            LOGGER.exception("Scan failed for %s", ticker)
            return None
        return None

    def _extract_ticker_frame(self, data, ticker):
        if isinstance(data.columns, pd.MultiIndex):
            if ticker not in data.columns.get_level_values(0):
                return None
            frame = data[ticker].dropna(how="all")
            return frame
        return data.dropna(how="all")

    def scan_universe(self, tickers, batch_size=100, pause_seconds=1):
        signals = []
        for start in range(0, len(tickers), batch_size):
            batch = tickers[start:start + batch_size]
            LOGGER.info("Downloading batch %s-%s (%s tickers)", start + 1, start + len(batch), len(batch))
            data = yf.download(
                tickers=" ".join(batch),
                period="1y",
                interval="1d",
                group_by="ticker",
                progress=False,
            )
            for ticker in batch:
                frame = self._extract_ticker_frame(data, ticker)
                if frame is None or frame.empty:
                    continue
                signal = self._scan_dataframe(ticker, frame)
                if signal:
                    signals.append(signal)
            time.sleep(pause_seconds)
        return signals

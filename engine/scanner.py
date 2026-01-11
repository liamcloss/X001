"""Technical scanner and risk engine for Trading 212 universe."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import pandas as pd
import yfinance as yf

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Signal:
    """Represents a high-conviction signal."""

    ticker: str
    entry: float
    target: float
    stop: float
    position_size: float
    rsi: float
    atr: float
    volume_ratio: float
    news: List[str]
    earnings_date: Optional[str]


class Throttler:
    """Rate limiter for YFinance requests."""

    def __init__(self, chunk_size: int = 40, sleep_seconds: int = 3) -> None:
        self.chunk_size = chunk_size
        self.sleep_seconds = sleep_seconds

    def chunk(self, tickers: List[str]) -> Iterable[List[str]]:
        """Yield tickers in chunks."""

        for index in range(0, len(tickers), self.chunk_size):
            yield tickers[index : index + self.chunk_size]

    def sleep(self) -> None:
        """Sleep between chunks to avoid rate limiting."""

        LOGGER.debug("Throttling YFinance calls for %ss", self.sleep_seconds)
        time.sleep(self.sleep_seconds)


class Scanner:
    """Scanner for momentum-based signals."""

    def __init__(self, capital: float) -> None:
        self.capital = capital
        self.throttler = Throttler()

    @staticmethod
    def _calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI indicator."""

        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = -delta.where(delta < 0, 0.0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def _calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Calculate Average True Range."""

        high_low = df["High"] - df["Low"]
        high_close = (df["High"] - df["Close"].shift()).abs()
        low_close = (df["Low"] - df["Close"].shift()).abs()
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        atr = true_range.rolling(window=period).mean()
        return atr

    def _position_size(self, entry: float, stop: float) -> float:
        """Calculate position size to risk <= 5% of total capital."""

        risk_per_trade = self.capital * 0.05
        risk_per_share = max(entry - stop, 0.01)
        shares = risk_per_trade / risk_per_share
        return round(shares, 2)

    def _liquidity_filter(self, df: pd.DataFrame) -> bool:
        """Check if 20-day average daily volume exceeds £500k."""

        avg_value = (df["Close"] * df["Volume"]).rolling(window=20).mean().iloc[-1]
        return avg_value >= 500_000

    def _momentum_signal(self, df: pd.DataFrame) -> Optional[Dict[str, float]]:
        """Check if a ticker meets momentum criteria."""

        close = df["Close"]
        rsi = self._calculate_rsi(close)
        sma_200 = close.rolling(window=200).mean()
        avg_volume = df["Volume"].rolling(window=20).mean()
        latest = df.iloc[-1]

        if pd.isna(sma_200.iloc[-1]) or pd.isna(rsi.iloc[-1]):
            return None
        if latest["Close"] <= sma_200.iloc[-1]:
            return None

        if not (rsi.iloc[-2] < 50 <= rsi.iloc[-1]):
            return None

        volume_ratio = latest["Volume"] / avg_volume.iloc[-1] if avg_volume.iloc[-1] else 0
        if volume_ratio <= 2:
            return None

        atr = self._calculate_atr(df).iloc[-1]
        if pd.isna(atr):
            return None

        return {
            "entry": latest["Close"],
            "rsi": rsi.iloc[-1],
            "atr": atr,
            "volume_ratio": volume_ratio,
        }

    def _fetch_news(self, ticker: str) -> List[str]:
        """Fetch the latest news headlines from YFinance."""

        try:
            yf_ticker = yf.Ticker(ticker)
            news_items = yf_ticker.news or []
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch news for %s: %s", ticker, exc)
            return []
        return [item.get("title", "") for item in news_items[:3] if item.get("title")]

    def _fetch_earnings_date(self, ticker: str) -> Optional[str]:
        """Fetch the next earnings date."""

        try:
            yf_ticker = yf.Ticker(ticker)
            calendar = yf_ticker.calendar
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch earnings date for %s: %s", ticker, exc)
            return None
        if isinstance(calendar, pd.DataFrame) and not calendar.empty:
            date_value = calendar.iloc[0, 0]
            return str(date_value.date())
        return None

    def scan(self, tickers: List[str]) -> List[Signal]:
        """Scan tickers for high-conviction signals."""

        signals: List[Signal] = []
        for chunk in self.throttler.chunk(tickers):
            LOGGER.info("Fetching market data for %s tickers", len(chunk))
            data = yf.download(
                tickers=" ".join(chunk),
                period="1y",
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
            for ticker in chunk:
                df = data[ticker] if ticker in data else None
                if df is None or df.empty:
                    LOGGER.debug("No data for %s", ticker)
                    continue
                if not self._liquidity_filter(df):
                    LOGGER.debug("Liquidity filter failed for %s", ticker)
                    continue
                momentum = self._momentum_signal(df)
                if not momentum:
                    continue
                entry = float(momentum["entry"])
                atr = float(momentum["atr"])
                stop = entry - (2.0 * atr)
                target = entry * 1.25
                position_size = self._position_size(entry, stop)
                news = self._fetch_news(ticker)
                earnings_date = self._fetch_earnings_date(ticker)
                signals.append(
                    Signal(
                        ticker=ticker,
                        entry=entry,
                        target=target,
                        stop=stop,
                        position_size=position_size,
                        rsi=float(momentum["rsi"]),
                        atr=atr,
                        volume_ratio=float(momentum["volume_ratio"]),
                        news=news,
                        earnings_date=earnings_date,
                    )
                )
            self.throttler.sleep()
        LOGGER.info("Scanner found %s signals", len(signals))
        return signals

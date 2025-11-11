"""Polygon.io intraday data client."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict

import pandas as pd
import requests
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)
EASTERN = ZoneInfo("US/Eastern")


@dataclass
class PolygonIntradayClient:
    """Client for fetching intraday data from Polygon.io."""

    api_key: str | None = None
    session: requests.Session | None = None

    def __post_init__(self) -> None:
        if self.api_key is None:
            self.api_key = os.environ.get("POLYGON_API_KEY")
        if not self.api_key:
            raise EnvironmentError("POLYGON_API_KEY environment variable is required.")
        self.session = self.session or requests.Session()
        self.base_url = "https://api.polygon.io"

    # ------------------------------------------------------------------
    def get_intraday_window(
        self,
        ticker: str,
        earnings_date: pd.Timestamp | datetime,
        release_flag: str,
    ) -> pd.DataFrame:
        """Fetch 5-minute bars for the earnings reaction window."""

        release_flag = (release_flag or "").upper()
        if release_flag not in {"AMC", "BMO"}:
            raise ValueError("release_flag must be 'AMC' or 'BMO'.")

        session_day = self.get_session_day(earnings_date, release_flag)

        start_dt = self._combine_day_time(session_day, time(9, 30), EASTERN)
        end_dt = self._combine_day_time(session_day, time(16, 0), EASTERN)
        return self._fetch_agg_bars(ticker, start_dt, end_dt)

    # ------------------------------------------------------------------
    def get_session_day(self, earnings_date: pd.Timestamp | datetime, release_flag: str) -> date:
        """Determine the trading session date for the earnings release."""

        release_flag = (release_flag or "").upper()
        if release_flag not in {"AMC", "BMO"}:
            raise ValueError("release_flag must be 'AMC' or 'BMO'.")

        earnings_ts = self._normalize_timestamp(earnings_date)
        if release_flag == "AMC":
            return self._next_trading_day(earnings_ts.date())
        return earnings_ts.date()

    # ------------------------------------------------------------------
    def get_previous_close(self, ticker: str, session_day: date) -> float | None:
        """Fetch the previous session close for ``ticker``."""

        prev_day = self._previous_trading_day(session_day)
        start_dt = self._combine_day_time(prev_day, time(0, 0), timezone.utc)
        end_dt = self._combine_day_time(prev_day, time(23, 59, 59), timezone.utc)
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": "2",
            "apiKey": self.api_key,
        }
        path = f"/v2/aggs/ticker/{ticker}/range/1/day/{self._to_millis(start_dt)}/{self._to_millis(end_dt)}"
        data = self._request(path, params=params)
        results = data.get("results", []) if isinstance(data, dict) else []
        if not results:
            LOGGER.warning("No previous close data for %s on %s", ticker, prev_day)
            return None
        close = results[-1].get("c")
        return float(close) if close is not None else None

    # ------------------------------------------------------------------
    def get_regular_session(self, ticker: str, session_day: date) -> pd.DataFrame:
        """Fetch 5-minute bars for a regular trading session."""

        start_dt = self._combine_day_time(session_day, time(9, 30), EASTERN)
        end_dt = self._combine_day_time(session_day, time(16, 0), EASTERN)
        return self._fetch_agg_bars(ticker, start_dt, end_dt)

    # ------------------------------------------------------------------
    def get_daily_bars(self, ticker: str, start_day: date, end_day: date) -> pd.DataFrame:
        """Fetch daily aggregated bars for ``ticker`` between ``start_day`` and ``end_day``."""

        start_dt = self._combine_day_time(start_day, time(0, 0), timezone.utc)
        end_dt = self._combine_day_time(end_day, time(23, 59, 59), timezone.utc)
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": "5000",
            "apiKey": self.api_key,
        }
        path = f"/v2/aggs/ticker/{ticker}/range/1/day/{self._to_millis(start_dt)}/{self._to_millis(end_dt)}"
        data = self._request(path, params=params)
        results = data.get("results", []) if isinstance(data, dict) else []
        if not results:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame(results)
        df = df.rename(columns={"t": "timestamp", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(EASTERN).dt.date
        df = df[["date", "open", "high", "low", "close", "volume"]]
        return df

    # ------------------------------------------------------------------
    def _fetch_agg_bars(self, ticker: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": "50000",
            "apiKey": self.api_key,
        }
        path = f"/v2/aggs/ticker/{ticker}/range/5/minute/{self._to_millis(start_dt)}/{self._to_millis(end_dt)}"
        data = self._request(path, params=params)
        results = data.get("results", []) if isinstance(data, dict) else []
        if not results:
            LOGGER.warning("No intraday bars returned for %s between %s and %s", ticker, start_dt, end_dt)
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(results)
        df = df.rename(columns={"t": "timestamp", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(EASTERN)
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        return df

    # ------------------------------------------------------------------
    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_timestamp(value: pd.Timestamp | datetime) -> datetime:
        ts = pd.to_datetime(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(timezone.utc)
        return ts.tz_convert(EASTERN)

    @staticmethod
    def _combine_day_time(day: date, tm: time, tz: ZoneInfo | timezone) -> datetime:
        return datetime.combine(day, tm, tzinfo=tz)

    @staticmethod
    def _to_millis(dt: datetime) -> int:
        if dt.tzinfo is None:
            raise ValueError("Datetime must be timezone-aware")
        return int(dt.timestamp() * 1000)

    @staticmethod
    def _next_trading_day(day: date) -> date:
        next_day = day + timedelta(days=1)
        while next_day.weekday() >= 5:  # Saturday=5, Sunday=6
            next_day += timedelta(days=1)
        return next_day

    @staticmethod
    def _previous_trading_day(day: date) -> date:
        prev_day = day - timedelta(days=1)
        while prev_day.weekday() >= 5:
            prev_day -= timedelta(days=1)
        return prev_day

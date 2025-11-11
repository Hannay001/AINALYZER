"""Earnings reaction analyzer."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List

import pandas as pd

from .intraday import PolygonIntradayClient

LOGGER = logging.getLogger(__name__)


@dataclass
class EarningsReactionAnalyzer:
    """Compute intraday reaction metrics for earnings events."""

    intraday_client: PolygonIntradayClient

    def analyze_ticker(self, ticker: str, events: pd.DataFrame) -> pd.DataFrame:
        """Compute reaction metrics for each event in ``events``."""

        if events.empty:
            return pd.DataFrame(columns=[
                "ticker",
                "earnings_date",
                "release_flag",
                "session_day",
                "gap_ret",
                "oc_ret",
                "range_pct",
                "open",
                "close",
                "high",
                "low",
                "prev_close",
            ])

        records: List[dict] = []
        events_sorted = events.sort_values("earnings_date")
        for _, event in events_sorted.iterrows():
            release_flag = str(event.get("release_flag", "")).upper()
            if release_flag not in {"AMC", "BMO"}:
                LOGGER.debug("Skipping event without supported release flag for %s", ticker)
                continue

            earnings_date = event.get("earnings_date")
            if pd.isna(earnings_date):
                continue

            session_day = self.intraday_client.get_session_day(earnings_date, release_flag)
            intraday_df = self.intraday_client.get_intraday_window(ticker, earnings_date, release_flag)
            if intraday_df.empty:
                LOGGER.warning("No intraday data for %s on %s", ticker, session_day)
                continue

            open_price = float(intraday_df.iloc[0]["open"])
            close_price = float(intraday_df.iloc[-1]["close"])
            high_price = float(intraday_df["high"].max())
            low_price = float(intraday_df["low"].min())

            prev_close = self.intraday_client.get_previous_close(ticker, session_day)

            gap_ret = pd.NA
            oc_ret = pd.NA
            range_pct = pd.NA

            if prev_close and prev_close != 0:
                gap_ret = open_price / prev_close - 1

            if open_price != 0:
                oc_ret = close_price / open_price - 1
                range_pct = (high_price - low_price) / open_price

            records.append({
                "ticker": ticker,
                "earnings_date": pd.to_datetime(earnings_date),
                "release_flag": release_flag,
                "session_day": session_day,
                "gap_ret": gap_ret,
                "oc_ret": oc_ret,
                "range_pct": range_pct,
                "open": open_price,
                "close": close_price,
                "high": high_price,
                "low": low_price,
                "prev_close": prev_close,
            })

        result = pd.DataFrame.from_records(records)
        return result

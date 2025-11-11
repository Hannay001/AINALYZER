"""Consistency scoring for earnings reactions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta

import pandas as pd

from .intraday import PolygonIntradayClient

LOGGER = logging.getLogger(__name__)


@dataclass
class ConsistencyScorer:
    """Score tickers based on earnings-day performance consistency."""

    intraday_client: PolygonIntradayClient
    sample_non_earnings_days: int = 10
    non_earnings_lookback_days: int = 120

    def score(self, ticker: str, events_metrics: pd.DataFrame) -> pd.Series:
        """Compute consistency metrics for ``ticker``."""

        if events_metrics.empty:
            return pd.Series(
                {
                    "hit_main_dir": 0.0,
                    "big_move_hit": 0.0,
                    "boost": 0.0,
                    "score": 0.0,
                    "events_count": 0,
                }
            )

        valid_events = events_metrics.dropna(subset=["oc_ret"]).copy()
        events_count = len(valid_events)
        if events_count == 0:
            return pd.Series(
                {
                    "hit_main_dir": 0.0,
                    "big_move_hit": 0.0,
                    "boost": 0.0,
                    "score": 0.0,
                    "events_count": 0,
                }
            )

        mean_oc = valid_events["oc_ret"].mean()
        sign_mean = 0
        if mean_oc > 0:
            sign_mean = 1
        elif mean_oc < 0:
            sign_mean = -1

        if sign_mean == 0:
            hit_main_dir = 0.0
        else:
            signs = valid_events["oc_ret"].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
            hit_main_dir = (signs == sign_mean).mean()

        big_move_hit = (valid_events["oc_ret"].abs() >= 0.02).mean()
        earnings_median_abs = valid_events["oc_ret"].abs().median()
        non_earnings_median_abs = self._median_abs_oc_non_earnings(ticker, valid_events)

        if non_earnings_median_abs and non_earnings_median_abs > 0:
            boost = earnings_median_abs / non_earnings_median_abs
        else:
            boost = 0.0

        score = 0.5 * hit_main_dir + 0.3 * big_move_hit + 0.2 * boost
        return pd.Series(
            {
                "hit_main_dir": float(hit_main_dir),
                "big_move_hit": float(big_move_hit),
                "boost": float(boost),
                "score": float(score),
                "events_count": int(events_count),
            }
        )

    # ------------------------------------------------------------------
    def _median_abs_oc_non_earnings(self, ticker: str, events_metrics: pd.DataFrame) -> float:
        session_days = pd.to_datetime(events_metrics["session_day"]).dt.date
        if session_days.empty:
            return 0.0

        end_day = session_days.max()
        start_day = end_day - timedelta(days=self.non_earnings_lookback_days)
        daily_bars = self.intraday_client.get_daily_bars(ticker, start_day, end_day)
        if daily_bars.empty:
            LOGGER.warning("No daily bars for %s between %s and %s", ticker, start_day, end_day)
            return 0.0

        mask = ~daily_bars["date"].isin(set(session_days))
        candidates = daily_bars.loc[mask].sort_values("date", ascending=False).head(self.sample_non_earnings_days)
        if candidates.empty:
            return 0.0

        candidates = candidates.assign(oc_ret=lambda df: df["close"] / df["open"] - 1)
        return float(candidates["oc_ret"].abs().median())

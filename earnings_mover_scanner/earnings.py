"""Earnings data fetcher using yahoo_fin."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
import pandas as pd

try:  # pragma: no cover - import side effect
    from yahoo_fin import stock_info as si
except ImportError as exc:  # pragma: no cover - runtime guard
    raise ImportError(
        "yahoo_fin is required for YahooEarningsFetcher. Install via 'pip install yahoo_fin'."
    ) from exc

LOGGER = logging.getLogger(__name__)


class YahooEarningsFetcher:
    """Fetches historical earnings data for tickers using yahoo_fin."""

    def __init__(self, lookback_years: int = 2) -> None:
        self.lookback_years = lookback_years

    def fetch(self, ticker: str) -> pd.DataFrame:
        """Fetch earnings history for ``ticker``.

        Parameters
        ----------
        ticker:
            The ticker symbol to fetch earnings for.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing earnings events.
        """

        LOGGER.info("Fetching earnings history for %s", ticker)
        raw_history = si.get_earnings_history(ticker)
        if not raw_history:
            LOGGER.warning("No earnings history found for %s", ticker)
            return pd.DataFrame(columns=[
                "ticker",
                "earnings_date",
                "period",
                "eps",
                "eps_est",
                "surprise",
                "release_flag",
            ])

        df = pd.DataFrame(raw_history)
        df = df.assign(ticker=ticker)
        df["earnings_date"] = pd.to_datetime(df.get("startdatetime"))
        cutoff = datetime.now(timezone.utc) - timedelta(days=365 * self.lookback_years)
        df = df.loc[df["earnings_date"] >= cutoff]
        df = df.rename(
            columns={
                "ticker": "ticker",
                "period": "period",
                "epsactual": "eps",
                "epsestimate": "eps_est",
                "epssurprisepct": "surprise",
                "startdatetime": "startdatetime",
                "time": "release_flag",
            }
        )
        columns = ["ticker", "earnings_date", "period", "eps", "eps_est", "surprise", "release_flag"]
        for column in columns:
            if column not in df.columns:
                df[column] = pd.NA
        df = df[columns].dropna(subset=["earnings_date"])
        df = df.sort_values("earnings_date").reset_index(drop=True)
        LOGGER.debug("Fetched %d earnings events for %s", len(df), ticker)
        return df

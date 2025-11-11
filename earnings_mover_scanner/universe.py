"""Universe builder for earnings mover scanner."""

from __future__ import annotations

import logging
from typing import List

import pandas as pd
import requests

LOGGER = logging.getLogger(__name__)


class MarketCapUniverseBuilder:
    """Builds a universe of U.S. listed stocks ranked by market cap."""

    SOURCE_URL: str = "https://stockanalysis.com/list/biggest-companies/"

    def __init__(self, session: requests.Session | None = None) -> None:
        self._session = session or requests.Session()

    def fetch(self, top_n: int = 100) -> List[str]:
        """Fetch the top ``top_n`` tickers by market cap.

        Parameters
        ----------
        top_n:
            Number of tickers to return. Defaults to 100.

        Returns
        -------
        list[str]
            List of ticker symbols.
        """

        LOGGER.info("Fetching top %s tickers by market cap", top_n)
        response = self._session.get(self.SOURCE_URL, timeout=30)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        if not tables:
            raise ValueError("No tables found on universe source page.")
        table = tables[0]
        ticker_column = None
        for candidate in ("Symbol", "Ticker", "Ticker Symbol"):
            if candidate in table.columns:
                ticker_column = candidate
                break
        if ticker_column is None:
            raise ValueError("Ticker column not found in universe table.")
        tickers = (
            table[ticker_column]
            .astype(str)
            .str.strip()
            .loc[lambda s: s.str.fullmatch(r"[A-Z.]+")]
            .head(top_n)
            .tolist()
        )
        LOGGER.debug("Fetched tickers: %s", tickers)
        return tickers

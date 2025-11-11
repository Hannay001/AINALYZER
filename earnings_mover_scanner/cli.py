"""Command-line interface for scanning earnings movers."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import List

import pandas as pd

from .analyzer import EarningsReactionAnalyzer
from .consistency import ConsistencyScorer
from .earnings import YahooEarningsFetcher
from .intraday import PolygonIntradayClient
from .universe import MarketCapUniverseBuilder

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")


def parse_args(args: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan earnings day movers across top market-cap tickers.")
    parser.add_argument("--top", type=int, default=100, help="Number of top market-cap tickers to scan.")
    parser.add_argument("--years", type=int, default=2, help="Number of years of earnings history to consider.")
    parser.add_argument("--output", type=Path, default=Path("earnings_mover_scan.csv"), help="Path to output CSV file.")
    return parser.parse_args(args=args)


def _safe_numeric_mean(series: pd.Series) -> float | type(pd.NA):
    """Return the mean of a potentially non-numeric series, ignoring missing values."""

    if series.empty:
        return pd.NA

    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return float(numeric.mean(skipna=True))

    return pd.NA


def run_scan(top: int, years: int, output: Path) -> pd.DataFrame:
    configure_logging()

    builder = MarketCapUniverseBuilder()
    earnings_fetcher = YahooEarningsFetcher(lookback_years=years)
    intraday_client = PolygonIntradayClient()
    analyzer = EarningsReactionAnalyzer(intraday_client)
    scorer = ConsistencyScorer(intraday_client)

    tickers = builder.fetch(top_n=top)
    LOGGER.info("Scanning %d tickers", len(tickers))

    records = []
    for ticker in tickers:
        try:
            events = earnings_fetcher.fetch(ticker)
            metrics = analyzer.analyze_ticker(ticker, events)
            score_series = scorer.score(ticker, metrics)

            record = {
                "ticker": ticker,
                "events_count": score_series.get("events_count", 0),
                "hit_main_dir": score_series.get("hit_main_dir", 0.0),
                "big_move_hit": score_series.get("big_move_hit", 0.0),
                "boost": score_series.get("boost", 0.0),
                "score": score_series.get("score", 0.0),
                "mean_gap": _safe_numeric_mean(metrics["gap_ret"]) if not metrics.empty else pd.NA,
                "mean_oc": _safe_numeric_mean(metrics["oc_ret"]) if not metrics.empty else pd.NA,
                "mean_range": _safe_numeric_mean(metrics["range_pct"]) if not metrics.empty else pd.NA,
            }
            records.append(record)
        except Exception as exc:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to process %s: %s", ticker, exc)

    if not records:
        LOGGER.warning("No records generated.")
        return pd.DataFrame()

    results = pd.DataFrame(records)
    results = results.sort_values("score", ascending=False).reset_index(drop=True)
    results.to_csv(output, index=False)
    LOGGER.info("Results written to %s", output)

    top10 = results.head(10)
    if not top10.empty:
        print(top10[["ticker", "score", "hit_main_dir", "big_move_hit", "boost"]].to_string(index=False))
    else:
        print("No results to display.")

    return results


def main() -> None:
    args = parse_args()
    run_scan(top=args.top, years=args.years, output=args.output)


if __name__ == "__main__":
    main()

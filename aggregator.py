"""Aggregator for sector ETF holdings

Fetches daily holdings spreadsheets for given ETFs and prints out
comma-separated lists of the stock tickers held.

Usage:
    python aggregator.py

Extend the `ETFS` dictionary with additional ticker names and URLs.
"""

import io
import json
import sys
from typing import Dict, List

import pandas as pd
import requests

# list of ETF symbols to process
ETFS: List[str] = [
    "XLY",  # Consumer Discretionary
    "XLP",  # Consumer Staples
    "XLE",  # Energy
    "XLF",  # Financials
    "XLV",  # Health Care
    "XLI",  # Industrials
    "XLB",  # Materials
    "XLRE", # Real Estate
    "XLK",  # Technology
    "XLU",  # Utilities
    "XLC",  # Communication Services
]


def get_holdings_url(symbol: str) -> str:
    """Construct the holdings Excel URL based on SSGA's naming pattern.
    
    SSGA uses URLs like holdings-daily-us-en-{symbol}.xlsx, which appear
    to be updated daily with the latest data.
    """
    return f"https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{symbol.lower()}.xlsx"


def fetch_holdings(url: str) -> pd.DataFrame:
    """Download an Excel file from *url* and return a DataFrame.

    The spreadsheet delivered by SSGA typically contains four rows of
    preamble before the real header line; this helper uses ``header=4``
    to start reading at the proper row. If you hit a different layout
    for another fund, adjust this accordingly.
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    # skip the first four rows which contain descriptive text
    return pd.read_excel(io.BytesIO(resp.content), header=4)


def tickers_from_df(df: pd.DataFrame) -> pd.Series:
    """Extract a Series of tickers from the holdings DataFrame.

    The standard SSGA layout includes a ``Ticker`` column immediately
    after the ``Name`` field; this helper is intentionally permissive
    to allow for slight variations in capitalization or whitespace.

    We also drop any placeholder values such as ``"-"`` which appear
    when the fund doesn’t have a ticker for a row.
    """
    for col in df.columns:
        if "ticker" in str(col).lower():
            series = df[col].dropna().astype(str)
            # filter out common placeholders
            return series[~series.isin({"", "-"})]
    # if we didn't find a ticker column, provide some context for
    # debugging by listing available columns
    raise ValueError(f"Could not find a ticker column in holdings data. "
                     f"Available columns: {list(df.columns)}")


import json


def main(write_json: bool = False) -> None:
    results: Dict[str, list[str]] = {}

    for symbol in ETFS:
        try:
            print(f"Fetching holdings URL for {symbol}...")
            url = get_holdings_url(symbol)
            print(f"Downloading holdings for {symbol} from {url}...")
            df = fetch_holdings(url)
            tickers = tickers_from_df(df)
            symbols_list = tickers.tolist()
            results[symbol] = symbols_list
            csv = ",".join(symbols_list)
            print(f"{symbol}: {csv}\n")
        except Exception as exc:
            print(f"Error processing {symbol}: {exc}", file=sys.stderr)

    if write_json:
        with open("site/holdings.json", "w") as f:
            json.dump(results, f, indent=2)
        print("Wrote site/holdings.json")


if __name__ == "__main__":
    # allow a command‑line flag to generate json for the website
    write = "--json" in sys.argv
    main(write_json=write)

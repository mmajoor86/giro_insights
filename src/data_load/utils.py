import json
import logging
from pathlib import Path

import yfinance as yf

from src.constants.config import path_isin_tickers

logger = logging.getLogger(__name__)


def _load_isin_cache() -> dict[str, str]:
    """Load the ISIN→ticker cache from JSON."""
    path = Path(path_isin_tickers)
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _save_isin_cache(cache: dict[str, str]) -> None:
    """Persist the ISIN→ticker cache to JSON."""
    Path(path_isin_tickers).write_text(json.dumps(cache, indent=4) + "\n")


def isin_to_ticker(isins: list[str]) -> dict[str, str]:
    """Resolve ISINs to Yahoo Finance tickers.

    Looks up each ISIN in the local cache first. Unknown ISINs are resolved
    via yfinance and automatically added to the cache for future runs.
    """
    cache = _load_isin_cache()
    mapping = {}
    new_entries = False

    for isin in isins:
        if isin in cache:
            mapping[isin] = cache[isin]
            continue
        try:
            ticker = yf.Ticker(isin)
            info = ticker.info
            symbol = info.get("symbol") if info else None
            if symbol:
                mapping[isin] = symbol
                cache[isin] = symbol
                new_entries = True
            else:
                logger.warning("Could not resolve ISIN: %s", isin)
        except Exception:
            logger.warning("Could not resolve ISIN: %s", isin)

    if new_entries:
        _save_isin_cache(cache)

    return mapping

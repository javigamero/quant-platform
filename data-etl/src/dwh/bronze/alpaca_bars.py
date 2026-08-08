"""Bronze ingestion: Alpaca 1-minute bars → `bronze/fact_bars_raw`.

Append-only, untransformed copy of `GET /v2/stocks/bars`. Keeping the raw response is
what makes the rest of the pipeline replayable: a bug in a feature definition must never
force a re-download of ten years of history through a 200 req/min budget.

`adjustment` is always `raw` here. Alpaca applies adjustments with the split and dividend
factors known *today*, so an adjusted re-extraction after a new split would return
different prices for the same dates. Adjustment happens in silver, from a versioned
corporate actions table.
"""

import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow.dataset as pa_ds

from src.dwh import schemas
from src.storage import lake

_LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEFRAME = "1Min"
DEFAULT_FEED = "sip"
DEFAULT_ADJUSTMENT = "raw"
DEFAULT_CURRENCY = "USD"

# One request returns at most 10k data points; a month of 1-minute bars is ~8k.
PAGE_LIMIT = 10000


def next_month(day: date) -> date:
    """Returns the first day of the month following `day`."""

    if day.month == 12:
        return date(day.year + 1, 1, 1)

    return date(day.year, day.month + 1, 1)


def iter_month_windows(start: date, end: date):
    """Yields `(year, month, window_start, window_end)` covering `[start, end]` month by month.

    Requesting one month at a time keeps each write aligned with exactly one
    `symbol=/year=/month=` partition, which is what makes the ingestion idempotent.
    """

    if start > end:
        return

    cursor = date(start.year, start.month, 1)

    while cursor <= end:
        window_end = min(end, next_month(cursor) - timedelta(days=1))
        yield f"{cursor.year:04d}", f"{cursor.month:02d}", max(start, cursor), window_end
        cursor = next_month(cursor)


def build_bars_raw(bars: pd.DataFrame, feed: str, adjustment: str, currency: str, ingested_at: datetime=None):
    """Adds request provenance and partition keys to the API response, typed to the contract.

    Parameters
    ----------
    * bars: pandas.DataFrame
        Output of `MarketData.get_bars`.
    * feed, adjustment, currency: str
        The request parameters, stored per row so a partition populated with the wrong
        feed can be spotted in an audit instead of silently poisoning the silver layer.
    * ingested_at: datetime
        Job clock, UTC. Defaults to now.

    Returns
    -------
    pyarrow.Table matching `schemas.BARS_RAW_SCHEMA`.
    """

    frame = bars.copy()
    frame["timestamp_at"] = pd.to_datetime(frame["timestamp_at"], utc=True)
    frame["feed"] = feed
    frame["adjustment"] = adjustment
    frame["currency"] = currency
    frame["ingested_at"] = to_utc_timestamp(ingested_at or datetime.now(timezone.utc))
    frame["year"] = frame["timestamp_at"].dt.strftime("%Y")
    frame["month"] = frame["timestamp_at"].dt.strftime("%m")

    return schemas.cast_to_schema(frame, schemas.BARS_RAW_SCHEMA)


def to_utc_timestamp(value) -> pd.Timestamp:
    """Normalises a datetime to a UTC-aware `pandas.Timestamp`, assuming UTC when naive."""

    timestamp = pd.Timestamp(value)

    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def ingest_bars_raw(
    market,
    symbols: str,
    start,
    end,
    timeframe: str=DEFAULT_TIMEFRAME,
    feed: str=DEFAULT_FEED,
    adjustment: str=DEFAULT_ADJUSTMENT,
    currency: str=DEFAULT_CURRENCY,
    lake_root: str=None,
    is_overwrite: bool=True,
) -> pd.DataFrame:
    """Backfills `bronze/fact_bars_raw` month by month over `[start, end]`.

    Parameters
    ----------
    * market: src.extractions.alpaca.MarketData
    * symbols: str
        Comma-separated symbols. The schema is multi-symbol from day one so that adding
        SPY and QQQ later (needed for the market-context features) costs nothing.
    * start, end: date | str
        Inclusive bounds, `YYYY-MM-DD`.
    * is_overwrite: bool=True
        When False, months that already have data on disk are skipped, so an interrupted
        backfill can be resumed without re-hitting the API.

    Returns
    -------
    pandas.DataFrame, one row per (year, month) window with the number of bars written.
    """

    start = _to_date(start)
    end = _to_date(end)
    path = lake.get_table_path(schemas.BRONZE_LAYER, schemas.BARS_RAW_TABLE, lake_root)
    symbol_list = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]

    summary = []

    for year, month, window_start, window_end in iter_month_windows(start, end):
        is_present = all(
            lake.has_partition(path, {"symbol": symbol, "year": year, "month": month})
            for symbol in symbol_list
        )

        if is_present and not is_overwrite:
            _LOGGER.info("Skipping %s-%s, partition already present", year, month)
            summary.append({"year": year, "month": month, "row_count": 0, "is_skipped": True})
            continue

        bars = market.get_bars(
            symbol=symbols,
            timeframe=timeframe,
            start=f"{window_start.isoformat()}T00:00:00Z",
            end=f"{window_end.isoformat()}T23:59:59.999Z",
            adjustment=adjustment,
            currency=currency,
            feed=feed,
            limit=PAGE_LIMIT,
        )

        if bars.empty:
            _LOGGER.warning("No bars returned for %s-%s", year, month)
            summary.append({"year": year, "month": month, "row_count": 0, "is_skipped": False})
            continue

        table = build_bars_raw(bars, feed=feed, adjustment=adjustment, currency=currency)
        lake.write_partitions(table, path, schemas.PARTITION_COLUMNS)

        _LOGGER.info("Wrote %s bars for %s-%s", table.num_rows, year, month)
        summary.append({"year": year, "month": month, "row_count": table.num_rows, "is_skipped": False})

    return pd.DataFrame(summary, columns=["year", "month", "row_count", "is_skipped"])


def read_bars_raw(symbol: str=None, lake_root: str=None) -> pd.DataFrame:
    """Reads `bronze/fact_bars_raw` back as a pandas frame, optionally for one symbol."""

    path = lake.get_table_path(schemas.BRONZE_LAYER, schemas.BARS_RAW_TABLE, lake_root)
    filters = None if symbol is None else pa_ds.field("symbol") == symbol
    table = lake.read_table(path, filters=filters)

    if table.num_columns == 0:
        return pd.DataFrame(columns=schemas.BARS_RAW_SCHEMA.names)

    return schemas.to_pandas(table)


def _to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    return pd.Timestamp(value).date()

"""Bronze ingestion of the two reference tables the silver layer cannot work without.

* `dim_market_calendar` — without it there is no way to tell "a minute with no trades"
  from "the market was closed", and that distinction decides how gaps get filled.
  Early closes at 13:00 ET (Thanksgiving eve, 24 Dec) are real and frequent.
* `dim_corporate_actions` — AAPL's 4:1 split (2020-08-31) and its quarterly dividends
  both create discontinuities in raw prices. Storing them versioned is what makes the
  adjusted series reproducible, instead of depending on whatever Alpaca knows today.

Both tables are small and rewritten whole on every run, which keeps them idempotent.
"""

import logging
from datetime import date, datetime, time, timezone

import pandas as pd

from src.dwh import schemas
from src.dwh.bronze.alpaca_bars import to_utc_timestamp
from src.storage import lake

_LOGGER = logging.getLogger(__name__)

REGULAR_CLOSE_ET = time(16, 0)


def build_market_calendar(calendar: pd.DataFrame, ingested_at: datetime=None):
    """Types the `/v2/calendar` response and derives session length and early-close flag.

    Parameters
    ----------
    * calendar: pandas.DataFrame
        Output of `MarketData.get_calendar`.
    * ingested_at: datetime
        Job clock, UTC. Defaults to now.

    Returns
    -------
    pyarrow.Table matching `schemas.MARKET_CALENDAR_SCHEMA`.
    """

    frame = calendar.copy()
    frame["session_date"] = pd.to_datetime(frame["session_date"]).dt.date
    frame["open_et"] = frame["open_et"].map(parse_et_time)
    frame["close_et"] = frame["close_et"].map(parse_et_time)
    frame["session_minutes"] = [
        _minutes_between(open_et, close_et)
        for open_et, close_et in zip(frame["open_et"], frame["close_et"])
    ]
    frame["is_half_day"] = frame["close_et"] < REGULAR_CLOSE_ET
    frame["settlement_date"] = pd.to_datetime(frame.get("settlement_date"), errors="coerce").dt.date
    frame["ingested_at"] = to_utc_timestamp(ingested_at or datetime.now(timezone.utc))

    return schemas.cast_to_schema(frame, schemas.MARKET_CALENDAR_SCHEMA)


def build_corporate_actions(actions: pd.DataFrame, ingested_at: datetime=None):
    """Types the normalised corporate actions frame to the contract.

    Returns
    -------
    pyarrow.Table matching `schemas.CORPORATE_ACTIONS_SCHEMA`.
    """

    frame = actions.copy()
    frame["ex_date"] = pd.to_datetime(frame["ex_date"]).dt.date
    frame["ingested_at"] = to_utc_timestamp(ingested_at or datetime.now(timezone.utc))

    return schemas.cast_to_schema(frame, schemas.CORPORATE_ACTIONS_SCHEMA)


def ingest_market_calendar(market, start, end, lake_root: str=None) -> pd.DataFrame:
    """Downloads and overwrites `bronze/dim_market_calendar` for `[start, end]`."""

    calendar = market.get_calendar(start=_to_iso(start), end=_to_iso(end))

    if calendar.empty:
        _LOGGER.warning("Calendar request returned no sessions for %s..%s", start, end)
        return calendar

    table = build_market_calendar(calendar)
    path = lake.get_table_path(schemas.BRONZE_LAYER, schemas.MARKET_CALENDAR_TABLE, lake_root)
    lake.write_table(table, path)

    _LOGGER.info("Wrote %s trading sessions to %s", table.num_rows, path)

    return schemas.to_pandas(table)


def ingest_corporate_actions(market, symbols: str, start, end, lake_root: str=None) -> pd.DataFrame:
    """Downloads and overwrites `bronze/dim_corporate_actions` for `[start, end]`.

    The range is walked in yearly windows because the endpoint limits how wide a single
    request may be.
    """

    frames = []

    for window_start, window_end in _iter_year_windows(_to_date(start), _to_date(end)):
        actions = market.get_corporate_actions(
            symbol=symbols,
            start=window_start.isoformat(),
            end=window_end.isoformat(),
        )
        if not actions.empty:
            frames.append(actions)

    if not frames:
        _LOGGER.warning("No corporate actions found for %s in %s..%s", symbols, start, end)
        return pd.DataFrame(columns=schemas.CORPORATE_ACTIONS_SCHEMA.names)

    actions = pd.concat(frames, ignore_index=True).drop_duplicates(
        subset=["symbol", "ex_date", "type"], ignore_index=True
    )

    table = build_corporate_actions(actions)
    path = lake.get_table_path(schemas.BRONZE_LAYER, schemas.CORPORATE_ACTIONS_TABLE, lake_root)
    lake.write_table(table, path)

    _LOGGER.info("Wrote %s corporate actions to %s", table.num_rows, path)

    return schemas.to_pandas(table)


def read_market_calendar(lake_root: str=None) -> pd.DataFrame:
    """Reads `bronze/dim_market_calendar` back as a pandas frame."""

    return _read(schemas.MARKET_CALENDAR_TABLE, schemas.MARKET_CALENDAR_SCHEMA, lake_root)


def read_corporate_actions(lake_root: str=None) -> pd.DataFrame:
    """Reads `bronze/dim_corporate_actions` back as a pandas frame."""

    return _read(schemas.CORPORATE_ACTIONS_TABLE, schemas.CORPORATE_ACTIONS_SCHEMA, lake_root)


def parse_et_time(value) -> time:
    """Parses Alpaca's `'HH:MM'` calendar times into `datetime.time`."""

    if isinstance(value, time):
        return value

    hour, minute = str(value).split(":")[:2]

    return time(int(hour), int(minute))


def _minutes_between(open_et: time, close_et: time) -> int:
    open_minutes = open_et.hour * 60 + open_et.minute
    close_minutes = close_et.hour * 60 + close_et.minute

    return close_minutes - open_minutes


def _iter_year_windows(start: date, end: date):
    cursor = start

    while cursor <= end:
        window_end = min(end, date(cursor.year, 12, 31))
        yield cursor, window_end
        cursor = date(cursor.year + 1, 1, 1)


def _read(table_name: str, schema, lake_root: str=None) -> pd.DataFrame:
    path = lake.get_table_path(schemas.BRONZE_LAYER, table_name, lake_root)
    table = lake.read_table(path)

    if table.num_columns == 0:
        return pd.DataFrame(columns=schema.names)

    return schemas.to_pandas(table)


def _to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    return pd.Timestamp(value).date()


def _to_iso(value) -> str:
    return _to_date(value).isoformat()

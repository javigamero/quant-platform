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
from src.storage.catalog import UnityCatalog, get_catalog

_LOGGER = logging.getLogger(__name__)

REGULAR_CLOSE_ET = time(16, 0)
ET_TIME_FORMAT = "%H:%M"


def build_market_calendar(calendar: pd.DataFrame, ingested_at: datetime=None):
    """Types the `/v2/calendar` response and derives session length and early-close flag.

    The session bounds are stored as `'HH:MM'` strings: Delta has no TIME type, and
    `read_market_calendar` parses them back into `datetime.time` for compute.

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
    open_et = frame["open_et"].map(parse_et_time)
    close_et = frame["close_et"].map(parse_et_time)
    frame["session_minutes"] = [
        _minutes_between(open_time, close_time) for open_time, close_time in zip(open_et, close_et)
    ]
    frame["is_half_day"] = close_et < REGULAR_CLOSE_ET
    frame["open_et"] = open_et.map(format_et_time)
    frame["close_et"] = close_et.map(format_et_time)
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


def ingest_market_calendar(market, start, end, lakehouse: UnityCatalog=None) -> pd.DataFrame:
    """Downloads and overwrites `lakehouse.bronze.dim_market_calendar` for `[start, end]`."""

    calendar = market.get_calendar(start=_to_iso(start), end=_to_iso(end))

    if calendar.empty:
        _LOGGER.warning("Calendar request returned no sessions for %s..%s", start, end)
        return calendar

    lakehouse = lakehouse or get_catalog()
    table = build_market_calendar(calendar)
    location = lakehouse.write_table(table, schemas.BRONZE_LAYER, schemas.MARKET_CALENDAR_TABLE)

    _LOGGER.info("Wrote %s trading sessions to %s", table.num_rows, location)

    return _to_calendar_frame(table)


def ingest_corporate_actions(market, symbols: str, start, end, lakehouse: UnityCatalog=None) -> pd.DataFrame:
    """Downloads and overwrites `lakehouse.bronze.dim_corporate_actions` for `[start, end]`.

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

    lakehouse = lakehouse or get_catalog()
    table = build_corporate_actions(actions)
    location = lakehouse.write_table(table, schemas.BRONZE_LAYER, schemas.CORPORATE_ACTIONS_TABLE)

    _LOGGER.info("Wrote %s corporate actions to %s", table.num_rows, location)

    return schemas.to_pandas(table)


def read_market_calendar(lakehouse: UnityCatalog=None) -> pd.DataFrame:
    """Reads `lakehouse.bronze.dim_market_calendar` back, with the session bounds as `time`."""

    frame = _read(schemas.MARKET_CALENDAR_TABLE, schemas.MARKET_CALENDAR_SCHEMA, lakehouse)

    if frame.empty:
        return frame

    return _parse_calendar_times(frame)


def read_corporate_actions(lakehouse: UnityCatalog=None) -> pd.DataFrame:
    """Reads `lakehouse.bronze.dim_corporate_actions` back as a pandas frame."""

    return _read(schemas.CORPORATE_ACTIONS_TABLE, schemas.CORPORATE_ACTIONS_SCHEMA, lakehouse)


def parse_et_time(value) -> time:
    """Parses Alpaca's `'HH:MM'` calendar times into `datetime.time`."""

    if isinstance(value, time):
        return value

    hour, minute = str(value).split(":")[:2]

    return time(int(hour), int(minute))


def format_et_time(value: time) -> str:
    """Renders a `datetime.time` as the `'HH:MM'` the contract stores."""

    return parse_et_time(value).strftime(ET_TIME_FORMAT)


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


def _read(table_name: str, schema, lakehouse: UnityCatalog=None) -> pd.DataFrame:
    lakehouse = lakehouse or get_catalog()
    table = lakehouse.read_table(schemas.BRONZE_LAYER, table_name)

    if table.num_columns == 0:
        return pd.DataFrame(columns=schema.names)

    return schemas.to_pandas(schemas.align_to_schema(table, schema))


def _to_calendar_frame(table) -> pd.DataFrame:
    """Hands the calendar back in its compute form, as `read_market_calendar` would."""

    return _parse_calendar_times(schemas.to_pandas(table))


def _parse_calendar_times(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["open_et"] = frame["open_et"].map(parse_et_time)
    frame["close_et"] = frame["close_et"].map(parse_et_time)

    return frame


def _to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    return pd.Timestamp(value).date()


def _to_iso(value) -> str:
    return _to_date(value).isoformat()

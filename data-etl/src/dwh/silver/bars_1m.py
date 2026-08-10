"""Silver layer: `lakehouse.bronze.fact_bars_raw` → `lakehouse.silver.fact_bars_1m`.

One row per minute of every regular trading session (09:30–15:59 ET, 390 rows on a full
day, 210 on an early close), produced as the cross product of the market calendar and the
minutes of each session, with the bronze bars left-joined onto it. The result has no gaps
by construction, which is what lets the gold layer use fixed-length rolling windows.

Adjustment is applied here, not at extraction time, from the versioned corporate actions
table — so the same code and the same bronze data always yield the same adjusted series.
"""

import logging
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

from src.dwh import schemas
from src.dwh.bronze.alpaca_bars import to_utc_timestamp
from src.storage.catalog import UnityCatalog, get_catalog

_LOGGER = logging.getLogger(__name__)

MARKET_TIMEZONE = "America/New_York"
REGULAR_SESSION_MINUTES = 390
HALF_SESSION_MINUTES = 210

# A 1-minute move this large is a corporate action, not a price: it must be explainable.
MAX_ABSOLUTE_RETURN = 0.20

# Actions whose price impact cannot be derived from the fields stored in the contract.
_UNSUPPORTED_ACTION_TYPES = ("merger", "spin_off")


class SilverValidationError(ValueError):
    """Raised when `silver/fact_bars_1m` violates the data contract. The job must fail, not warn."""


def build_session_grid(calendar: pd.DataFrame, symbols) -> pd.DataFrame:
    """Expands the market calendar into one row per (symbol, session minute).

    Parameters
    ----------
    * calendar: pandas.DataFrame
        `bronze/dim_market_calendar`: `session_date`, `open_et`, `session_minutes`, `is_half_day`.
    * symbols: list | str
        Symbols to build the grid for.

    Returns
    -------
    pandas.DataFrame with `symbol`, `timestamp_at` (UTC), `timestamp_et_at`, `session_date`,
    `minute_index` (0-based, from the session open) and `is_half_day`.
    """

    symbols = [symbols] if isinstance(symbols, str) else list(symbols)
    calendar = calendar.sort_values("session_date", ignore_index=True)

    if calendar.empty or not symbols:
        return pd.DataFrame(
            columns=["symbol", "timestamp_at", "timestamp_et_at", "session_date", "minute_index", "is_half_day"]
        )

    minute_counts = calendar["session_minutes"].to_numpy().astype("int64")
    session_opens = pd.to_datetime([
        datetime.combine(session_date, open_et)
        for session_date, open_et in zip(calendar["session_date"], calendar["open_et"])
    ])

    minute_index = np.concatenate([np.arange(count, dtype="int64") for count in minute_counts])
    opens_expanded = pd.DatetimeIndex(np.repeat(session_opens.to_numpy(), minute_counts))
    # Sessions never span the 02:00 ET DST switch, so localisation is unambiguous.
    timestamp_et = (opens_expanded + pd.to_timedelta(minute_index, unit="m")).tz_localize(MARKET_TIMEZONE)

    session_grid = pd.DataFrame({
        "timestamp_at": timestamp_et.tz_convert("UTC"),
        "timestamp_et_at": timestamp_et,
        "session_date": np.repeat(calendar["session_date"].to_numpy(), minute_counts),
        "minute_index": minute_index.astype("int16"),
        "is_half_day": np.repeat(calendar["is_half_day"].to_numpy(), minute_counts),
    })

    grid = pd.concat(
        [session_grid.assign(symbol=symbol) for symbol in symbols],
        ignore_index=True,
    )

    return grid[
        ["symbol", "timestamp_at", "timestamp_et_at", "session_date", "minute_index", "is_half_day"]
    ].sort_values(["symbol", "timestamp_at"], ignore_index=True)


def compute_adjustment_factors(
    session_dates, corporate_actions: pd.DataFrame, daily_close: pd.Series
) -> pd.DataFrame:
    """Builds the cumulative back-adjustment factors for every session date.

    A factor applies to sessions **strictly before** the ex-date: from the ex-date on, the
    market already trades at the adjusted price.

    * split (or stock dividend) with share ratio `R` → prices ×`1/R`, volume ×`R`
    * cash dividend of `d` with previous close `C` → prices ×`(1 - d/C)`

    Parameters
    ----------
    * session_dates: sequence of `datetime.date`
    * corporate_actions: pandas.DataFrame
        `bronze/dim_corporate_actions` rows for the symbol.
    * daily_close: pandas.Series
        Raw session close indexed by `session_date`, used to turn cash dividends into ratios.

    Returns
    -------
    pandas.DataFrame indexed by `session_date` with `adj_factor` (prices) and
    `split_factor` (the split-only part; volume is divided by it).
    """

    session_dates = pd.Index(sorted(set(session_dates)), name="session_date")
    factors = pd.DataFrame(
        {"adj_factor": np.ones(len(session_dates)), "split_factor": np.ones(len(session_dates))},
        index=session_dates,
    )

    if corporate_actions is None or corporate_actions.empty:
        return factors

    actions = corporate_actions.sort_values("ex_date")

    for action in actions.itertuples(index=False):
        action_type = getattr(action, "type")

        if action_type in _UNSUPPORTED_ACTION_TYPES:
            _LOGGER.warning(
                "Ignoring %s on %s: no adjustment factor can be derived from the stored fields",
                action_type, action.ex_date,
            )
            continue

        price_factor = _price_factor(action, daily_close)

        if price_factor is None:
            continue

        is_before_ex_date = session_dates < action.ex_date
        factors.loc[is_before_ex_date, "adj_factor"] *= price_factor

        if action_type in ("split", "stock_dividend"):
            factors.loc[is_before_ex_date, "split_factor"] *= price_factor

    return factors


def build_bars_1m(
    bars: pd.DataFrame,
    calendar: pd.DataFrame,
    corporate_actions: pd.DataFrame=None,
    start: date=None,
    end: date=None,
    ingested_at: datetime=None,
) -> pd.DataFrame:
    """Aligns bronze bars to the session grid, fills gaps and adds adjusted prices.

    Gap policy: a minute with no trades carries the previous close forward
    (`open = high = low = vwap = close`, `volume = 0`, `trade_count = 0`, `is_imputed = True`).
    Prices are never interpolated linearly — that invents movement that did not happen and
    contaminates every volatility estimate downstream.

    `is_imputed` is itself a feature (it marks illiquidity), so imputed rows are kept; it is
    the *target* that must exclude them, since their return is zero by construction.

    Parameters
    ----------
    * bars: pandas.DataFrame
        `bronze/fact_bars_raw` rows, one feed and one adjustment only.
    * calendar: pandas.DataFrame
        `bronze/dim_market_calendar`.
    * corporate_actions: pandas.DataFrame
        `bronze/dim_corporate_actions`. Without it every factor is 1.0.
    * start, end: date
        Session range to build. Defaults to the range covered by `bars`.

    Returns
    -------
    pandas.DataFrame with the columns of `schemas.BARS_1M_SCHEMA`.
    """

    if bars.empty:
        return pd.DataFrame(columns=schemas.BARS_1M_SCHEMA.names)

    bars = bars.copy()
    bars["timestamp_at"] = pd.to_datetime(bars["timestamp_at"], utc=True)

    feed = _single_value(bars, "feed")
    adjustment = _single_value(bars, "adjustment")
    symbols = sorted(bars["symbol"].unique())

    session_dates = bars["timestamp_at"].dt.tz_convert(MARKET_TIMEZONE).dt.date
    start = start or session_dates.min()
    end = end or session_dates.max()

    calendar = calendar[
        (calendar["session_date"] >= start) & (calendar["session_date"] <= end)
    ]

    frame = build_session_grid(calendar, symbols).merge(
        bars[["symbol", "timestamp_at", "open", "high", "low", "close", "volume", "trade_count", "vwap"]],
        on=["symbol", "timestamp_at"],
        how="left",
    )

    if frame.empty:
        return pd.DataFrame(columns=schemas.BARS_1M_SCHEMA.names)

    frame = _impute_missing_minutes(frame)
    frame = _add_adjusted_columns(frame, corporate_actions)

    frame["feed"] = feed
    frame["adjustment"] = adjustment
    frame["ingested_at"] = to_utc_timestamp(ingested_at or datetime.now(timezone.utc))
    frame["year"] = frame["timestamp_at"].dt.strftime("%Y")
    frame["month"] = frame["timestamp_at"].dt.strftime("%m")

    return frame[list(schemas.BARS_1M_SCHEMA.names)]


def validate_bars_1m(bars_1m: pd.DataFrame, calendar: pd.DataFrame, corporate_actions: pd.DataFrame=None) -> list:
    """Runs the mandatory silver checks and returns a list of violation messages.

    Checks, per the data contract:
    1. `low <= min(open, close) <= max(open, close) <= high`
    2. `vwap` inside `[low, high]`
    3. `volume > 0` if and only if `trade_count > 0`
    4. one row per session minute (390 on a full day, 210 on an early close)
    5. no duplicates on `(symbol, timestamp_at)`
    6. no 1-minute return above 20% that is not explained by a corporate action
    """

    violations = []

    if bars_1m.empty:
        return ["silver/fact_bars_1m is empty"]

    violations += _validate_ohlc_bounds(bars_1m)
    violations += _validate_vwap_bounds(bars_1m)
    violations += _validate_volume_trade_count(bars_1m)
    violations += _validate_session_row_counts(bars_1m, calendar)
    violations += _validate_uniqueness(bars_1m)
    violations += _validate_returns(bars_1m, corporate_actions)

    return violations


def assert_bars_1m_valid(bars_1m: pd.DataFrame, calendar: pd.DataFrame, corporate_actions: pd.DataFrame=None) -> None:
    """Raises `SilverValidationError` listing every contract violation found."""

    violations = validate_bars_1m(bars_1m, calendar, corporate_actions)

    if violations:
        raise SilverValidationError(
            f"{len(violations)} validation(s) failed for silver/fact_bars_1m:\n- "
            + "\n- ".join(violations)
        )


def run_bronze_to_silver(
    symbol: str,
    start=None,
    end=None,
    lakehouse: UnityCatalog=None,
    is_validated: bool=True,
) -> pd.DataFrame:
    """Reads bronze, builds `lakehouse.silver.fact_bars_1m` for one symbol and writes it.

    Parameters
    ----------
    * symbol: str
        A single symbol; the silver build is per-symbol so a backfill can be parallelised.
    * start, end: date | str
        Session range. Defaults to the range present in bronze.
    * lakehouse: src.storage.catalog.UnityCatalog
        Catalog holding both the bronze input and the silver output. Defaults to the one
        configured in the environment.
    * is_validated: bool=True
        When True (the default, and the contract), any violation raises and nothing is written.

    Returns
    -------
    The silver frame that was written.
    """

    from src.dwh.bronze import alpaca_bars, alpaca_reference

    lakehouse = lakehouse or get_catalog()
    bars = alpaca_bars.read_bars_raw(symbol=symbol, lakehouse=lakehouse)
    calendar = alpaca_reference.read_market_calendar(lakehouse=lakehouse)
    corporate_actions = alpaca_reference.read_corporate_actions(lakehouse=lakehouse)
    corporate_actions = corporate_actions[corporate_actions["symbol"] == symbol]

    bars_1m = build_bars_1m(
        bars=bars,
        calendar=calendar,
        corporate_actions=corporate_actions,
        start=_to_date(start),
        end=_to_date(end),
    )

    if bars_1m.empty:
        _LOGGER.warning("No silver rows produced for %s", symbol)
        return bars_1m

    if is_validated:
        assert_bars_1m_valid(bars_1m, calendar, corporate_actions)

    table = schemas.cast_to_schema(bars_1m, schemas.BARS_1M_SCHEMA)
    location = lakehouse.write_partitions(
        table, schemas.SILVER_LAYER, schemas.BARS_1M_TABLE, schemas.PARTITION_COLUMNS
    )

    _LOGGER.info("Wrote %s silver rows for %s to %s", table.num_rows, symbol, location)

    return bars_1m


def _impute_missing_minutes(frame: pd.DataFrame) -> pd.DataFrame:
    """Forward-fills the close into minutes with no bar; see the gap policy in `build_bars_1m`."""

    frame = frame.sort_values(["symbol", "timestamp_at"], ignore_index=True)
    frame["is_imputed"] = frame["close"].isna()
    frame["close"] = frame.groupby("symbol")["close"].ffill()

    # Minutes before a symbol's first ever bar have nothing to carry forward. Whole sessions
    # are dropped rather than individual minutes, so every surviving session stays complete.
    is_unfillable = frame["close"].isna()
    if is_unfillable.any():
        incomplete_sessions = set(map(tuple, frame.loc[is_unfillable, ["symbol", "session_date"]].to_numpy()))
        is_incomplete = pd.Series(
            list(map(tuple, frame[["symbol", "session_date"]].to_numpy())), index=frame.index
        ).isin(incomplete_sessions)
        _LOGGER.warning(
            "Dropping %s session(s) that start before the first available bar", len(incomplete_sessions)
        )
        frame = frame[~is_incomplete].reset_index(drop=True)

    is_imputed = frame["is_imputed"]

    for column in ("open", "high", "low", "vwap"):
        frame.loc[is_imputed, column] = frame.loc[is_imputed, "close"]

    frame.loc[is_imputed, "volume"] = 0
    frame.loc[is_imputed, "trade_count"] = 0

    frame["volume"] = frame["volume"].astype("int64")
    frame["trade_count"] = frame["trade_count"].astype("int32")

    return frame


def _add_adjusted_columns(frame: pd.DataFrame, corporate_actions: pd.DataFrame) -> pd.DataFrame:
    """Attaches `adj_factor` / `split_factor` and the adjusted price and volume columns.

    Factors are derived per symbol: both the corporate actions and the previous close a
    dividend is scaled against belong to one symbol only.
    """

    adjusted = []

    for symbol, symbol_frame in frame.groupby("symbol", sort=False):
        symbol_actions = _actions_for(corporate_actions, symbol)
        daily_close = (
            symbol_frame[~symbol_frame["is_imputed"]]
            .sort_values("timestamp_at")
            .groupby("session_date")["close"]
            .last()
        )
        factors = compute_adjustment_factors(symbol_frame["session_date"], symbol_actions, daily_close)

        adjusted.append(symbol_frame.merge(factors, left_on="session_date", right_index=True, how="left"))

    frame = pd.concat(adjusted, ignore_index=True).sort_values(
        ["symbol", "timestamp_at"], ignore_index=True
    )
    frame[["adj_factor", "split_factor"]] = frame[["adj_factor", "split_factor"]].fillna(1.0)

    for column in ("open", "high", "low", "close", "vwap"):
        frame[f"{column}_adj"] = frame[column] * frame["adj_factor"]

    frame["volume_adj"] = (frame["volume"] / frame["split_factor"]).round().astype("int64")

    return frame


def _actions_for(corporate_actions: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if corporate_actions is None or corporate_actions.empty:
        return corporate_actions

    if "symbol" not in corporate_actions.columns:
        return corporate_actions

    return corporate_actions[corporate_actions["symbol"] == symbol]


def _price_factor(action, daily_close: pd.Series):
    """Returns the multiplier applied to prices *before* the ex-date, or None if not derivable."""

    action_type = getattr(action, "type")

    if action_type in ("split", "stock_dividend"):
        ratio = getattr(action, "ratio", None)
        if ratio is None or pd.isna(ratio) or float(ratio) == 0:
            _LOGGER.warning("Skipping %s on %s: missing ratio", action_type, action.ex_date)
            return None
        return 1 / float(ratio)

    if action_type == "cash_dividend":
        cash_amount = getattr(action, "cash_amount", None)
        if cash_amount is None or pd.isna(cash_amount):
            return None

        previous_close = _previous_close(daily_close, action.ex_date)
        if previous_close is None or previous_close <= 0:
            _LOGGER.warning("Skipping dividend on %s: no prior close to scale it against", action.ex_date)
            return None

        return 1 - float(cash_amount) / float(previous_close)

    _LOGGER.warning("Unknown corporate action type '%s' on %s", action_type, action.ex_date)

    return None


def _previous_close(daily_close: pd.Series, ex_date: date):
    """Returns the raw close of the last session strictly before `ex_date`."""

    if daily_close is None or daily_close.empty:
        return None

    position = daily_close.index.searchsorted(ex_date, side="left")

    if position == 0:
        return None

    return daily_close.iloc[position - 1]


def _validate_ohlc_bounds(bars_1m: pd.DataFrame) -> list:
    body_low = bars_1m[["open", "close"]].min(axis=1)
    body_high = bars_1m[["open", "close"]].max(axis=1)
    is_invalid = (bars_1m["low"] > body_low) | (body_high > bars_1m["high"])

    if not is_invalid.any():
        return []

    return [f"{int(is_invalid.sum())} row(s) violate low <= min(open, close) <= max(open, close) <= high"]


def _validate_vwap_bounds(bars_1m: pd.DataFrame) -> list:
    is_invalid = (bars_1m["vwap"] < bars_1m["low"]) | (bars_1m["vwap"] > bars_1m["high"])

    if not is_invalid.any():
        return []

    return [f"{int(is_invalid.sum())} row(s) have vwap outside [low, high]"]


def _validate_volume_trade_count(bars_1m: pd.DataFrame) -> list:
    is_invalid = (bars_1m["volume"] > 0) != (bars_1m["trade_count"] > 0)

    if not is_invalid.any():
        return []

    return [f"{int(is_invalid.sum())} row(s) break the volume > 0 <=> trade_count > 0 invariant"]


def _validate_session_row_counts(bars_1m: pd.DataFrame, calendar: pd.DataFrame) -> list:
    expected = calendar.set_index("session_date")["session_minutes"]
    actual = bars_1m.groupby(["symbol", "session_date"]).size()

    violations = []

    for (symbol, session_date), row_count in actual.items():
        expected_count = expected.get(session_date)
        if expected_count is not None and row_count != expected_count:
            violations.append(
                f"{symbol} {session_date}: {row_count} rows, expected {int(expected_count)}"
            )

    if not violations:
        return []

    return [f"{len(violations)} session(s) with an unexpected row count, e.g. {violations[0]}"]


def _validate_uniqueness(bars_1m: pd.DataFrame) -> list:
    duplicate_count = int(bars_1m.duplicated(subset=["symbol", "timestamp_at"]).sum())

    if duplicate_count == 0:
        return []

    return [f"{duplicate_count} duplicate row(s) on (symbol, timestamp_at)"]


def _validate_returns(bars_1m: pd.DataFrame, corporate_actions: pd.DataFrame) -> list:
    frame = bars_1m.sort_values(["symbol", "timestamp_at"])
    grouped = frame.groupby(["symbol", "session_date"])["close_adj"]
    returns = np.log(frame["close_adj"] / grouped.shift(1))

    is_extreme = returns.abs() > MAX_ABSOLUTE_RETURN

    if corporate_actions is not None and not corporate_actions.empty:
        # A factor applied one session off would show up as a jump on the ex-date's edges.
        event_dates = set()
        for ex_date in corporate_actions["ex_date"]:
            event_dates.update({ex_date - timedelta(days=1), ex_date, ex_date + timedelta(days=1)})
        is_extreme &= ~frame["session_date"].isin(event_dates)

    if not is_extreme.any():
        return []

    first = frame.loc[is_extreme].iloc[0]

    return [
        f"{int(is_extreme.sum())} unexplained 1-minute return(s) above "
        f"{MAX_ABSOLUTE_RETURN:.0%}, e.g. {first['symbol']} at {first['timestamp_at']}"
    ]


def _single_value(frame: pd.DataFrame, column: str):
    values = frame[column].dropna().unique()

    if len(values) > 1:
        raise ValueError(
            f"Expected a single '{column}' in the bronze input, found {sorted(values)}. "
            "Silver must be built per feed/adjustment so the logical key stays unique."
        )

    return values[0] if len(values) else None


def _to_date(value):
    if value is None or (isinstance(value, date) and not isinstance(value, datetime)):
        return value

    return pd.Timestamp(value).date()

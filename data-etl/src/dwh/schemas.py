"""Canonical Arrow schemas for the Alpaca 1-minute bar pipeline.

These schemas are the versioned contract between layers: a breaking change means
bumping `SCHEMA_VERSION` and writing to a new path, never mutating the existing one.

Design notes (see `configs/data_contracts/alpaca_bars.md`):
* Prices are `decimal(18,6)`, not `float`: accumulating log-returns over ~1M bars
  amplifies binary rounding error.
* Timestamps are stored in UTC. `America/New_York` only appears in the silver layer,
  where the market calendar (DST, 09:30 ET open) makes local time meaningful.
* Table names follow the repo's Kimball convention; the design document calls them
  `bars_raw`, `bars_1m`, `market_calendar` and `corporate_actions`.
"""

import pyarrow as pa

SCHEMA_VERSION = "v1"

BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"

BARS_RAW_TABLE = "fact_bars_raw"
BARS_1M_TABLE = "fact_bars_1m"
MARKET_CALENDAR_TABLE = "dim_market_calendar"
CORPORATE_ACTIONS_TABLE = "dim_corporate_actions"

PARTITION_COLUMNS = ["symbol", "year", "month"]

_PRICE = pa.decimal128(18, 6)
_FACTOR = pa.decimal128(18, 10)
_TIMESTAMP_UTC = pa.timestamp("us", tz="UTC")
_TIMESTAMP_ET = pa.timestamp("us", tz="America/New_York")

PRICE_SCALE = 6
FACTOR_SCALE = 10

# bronze/fact_bars_raw — the literal API response plus request provenance.
BARS_RAW_SCHEMA = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("timestamp_at", _TIMESTAMP_UTC, nullable=False),
    pa.field("open", _PRICE),
    pa.field("high", _PRICE),
    pa.field("low", _PRICE),
    pa.field("close", _PRICE),
    pa.field("volume", pa.int64()),
    pa.field("trade_count", pa.int32()),
    pa.field("vwap", _PRICE),
    pa.field("feed", pa.string(), nullable=False),
    pa.field("adjustment", pa.string(), nullable=False),
    pa.field("currency", pa.string()),
    pa.field("ingested_at", _TIMESTAMP_UTC, nullable=False),
    pa.field("year", pa.string(), nullable=False),
    pa.field("month", pa.string(), nullable=False),
])

# bronze/dim_market_calendar — one row per trading session.
MARKET_CALENDAR_SCHEMA = pa.schema([
    pa.field("session_date", pa.date32(), nullable=False),
    pa.field("open_et", pa.time32("s"), nullable=False),
    pa.field("close_et", pa.time32("s"), nullable=False),
    pa.field("session_minutes", pa.int16(), nullable=False),
    pa.field("is_half_day", pa.bool_(), nullable=False),
    pa.field("settlement_date", pa.date32()),
    pa.field("ingested_at", _TIMESTAMP_UTC, nullable=False),
])

# bronze/dim_corporate_actions — splits and dividends, the source of price discontinuities.
CORPORATE_ACTIONS_SCHEMA = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("ex_date", pa.date32(), nullable=False),
    pa.field("type", pa.string(), nullable=False),
    pa.field("ratio", _FACTOR),
    pa.field("cash_amount", _PRICE),
    pa.field("ingested_at", _TIMESTAMP_UTC, nullable=False),
])

# silver/fact_bars_1m — one row per minute of every trading session, no gaps.
BARS_1M_SCHEMA = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("timestamp_at", _TIMESTAMP_UTC, nullable=False),
    pa.field("timestamp_et_at", _TIMESTAMP_ET, nullable=False),
    pa.field("session_date", pa.date32(), nullable=False),
    pa.field("minute_index", pa.int16(), nullable=False),
    pa.field("open", _PRICE),
    pa.field("high", _PRICE),
    pa.field("low", _PRICE),
    pa.field("close", _PRICE),
    pa.field("volume", pa.int64()),
    pa.field("trade_count", pa.int32()),
    pa.field("vwap", _PRICE),
    pa.field("adj_factor", _FACTOR, nullable=False),
    pa.field("split_factor", _FACTOR, nullable=False),
    pa.field("open_adj", _PRICE),
    pa.field("high_adj", _PRICE),
    pa.field("low_adj", _PRICE),
    pa.field("close_adj", _PRICE),
    pa.field("vwap_adj", _PRICE),
    pa.field("volume_adj", pa.int64()),
    pa.field("is_imputed", pa.bool_(), nullable=False),
    pa.field("is_half_day", pa.bool_(), nullable=False),
    pa.field("feed", pa.string(), nullable=False),
    pa.field("adjustment", pa.string(), nullable=False),
    pa.field("ingested_at", _TIMESTAMP_UTC, nullable=False),
    pa.field("year", pa.string(), nullable=False),
    pa.field("month", pa.string(), nullable=False),
])


def cast_to_schema(df, schema: pa.Schema) -> pa.Table:
    """Converts a pandas frame to an Arrow table matching `schema` exactly.

    Float columns are rounded to their target decimal scale first: casting a binary
    float straight to `decimal128` fails when the value has no exact representation.
    """

    df = df.copy()

    for field in schema:
        if field.name not in df.columns:
            raise KeyError(f"Column '{field.name}' is missing from the frame; schema requires it.")

        if pa.types.is_decimal(field.type):
            df[field.name] = df[field.name].astype("float64").round(field.type.scale)

    table = pa.Table.from_pandas(df[schema.names], preserve_index=False)

    return table.cast(schema)


def to_pandas(table: pa.Table):
    """Converts a lake table to pandas, widening decimals to `float64`.

    Arrow hands `decimal128` back as Python `Decimal` objects, which silently break every
    arithmetic operation downstream. Decimal is the storage contract; float is the compute type.
    """

    fields = [
        field.with_type(pa.float64()) if pa.types.is_decimal(field.type) else field
        for field in table.schema
    ]

    return table.cast(pa.schema(fields)).to_pandas()

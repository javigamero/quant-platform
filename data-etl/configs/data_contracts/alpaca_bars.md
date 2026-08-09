# Data contract — Alpaca 1-minute bars (`v1`)

Field-level contract for the AAPL 1-minute pipeline. The authoritative definition is
`src/dwh/schemas.py`; this file is its readable form. A breaking change means bumping
`SCHEMA_VERSION` and writing to a **new table** — never mutating an existing one.

Every table is an external Delta table registered in Unity Catalog, so the layer is part
of the name. Table names follow the repo's Kimball convention; the design document refers
to them without the prefix:

| Design document | This repo | Unity Catalog |
|---|---|---|
| `bars_raw` | `fact_bars_raw` | `lakehouse.bronze.fact_bars_raw` |
| `market_calendar` | `dim_market_calendar` | `lakehouse.bronze.dim_market_calendar` |
| `corporate_actions` | `dim_corporate_actions` | `lakehouse.bronze.dim_corporate_actions` |
| `bars_1m` | `fact_bars_1m` | `lakehouse.silver.fact_bars_1m` |

## Fixed decisions

| Decision | Value | Why |
|---|---|---|
| Source | Alpaca Market Data v2, `feed=sip` | 100% of the volume; on the Basic plan for data older than 15 minutes. |
| Granularity | `1Min` | ~1M rows for AAPL 2016→2026; re-aggregable to 5m/15m/1h/1d without touching the API. |
| Adjustment | `raw` in bronze, applied in silver | Alpaca adjusts with the factors known *today*, so an adjusted re-extraction after a new split returns different prices for the same dates. |
| Format | Delta Lake (Parquet + Snappy files), external table in Unity Catalog | The transaction log makes a partition replacement atomic, so a retried write cannot duplicate rows; the catalog makes the same table readable from pandas, Spark and the UC UI. |
| Timezone | UTC in storage, `America/New_York` in silver | The market calendar (DST, 09:30 ET open) only makes sense in local time. |
| Price type | `decimal(18,6)` | Accumulating log-returns over ~1M bars amplifies binary float rounding. Decimal is the storage type; `schemas.to_pandas` widens to `float64` for compute. |

## bronze / `fact_bars_raw`

Literal response of `GET /v2/stocks/bars?symbols=AAPL&timeframe=1Min&feed=sip&adjustment=raw`,
plus request provenance.

| Column | Type | Origin | Notes |
|---|---|---|---|
| `symbol` | `string` | payload key | The schema is multi-symbol from day one. |
| `timestamp_at` | `timestamp[us, UTC]` | `t` | RFC-3339 from the API. Marks the **start** of the bar. |
| `open`, `high`, `low`, `close` | `decimal(18,6)` | `o`, `h`, `l`, `c` | |
| `volume` | `int64` | `v` | Shares. |
| `trade_count` | `int32` | `n` | Trades in the bar. An underrated feature. |
| `vwap` | `decimal(18,6)` | `vw` | VWAP of the bar. |
| `feed` | `string` | request | `sip` / `iex`. Lets an audit catch a partition populated with the wrong feed. |
| `adjustment` | `string` | request | Always `raw` in this layer. |
| `currency` | `string` | request | ISO 4217, `USD`. |
| `ingested_at` | `timestamp[us, UTC]` | job clock | Traceability and re-ingestion detection. |
| `year`, `month` | `string` | derived from `timestamp_at` | Partition keys, zero-padded. |

* **Partitioning:** `symbol=AAPL/year=2024/month=03/` — keeps files at ~5–20 MB and allows selective backfills.
* **Logical key:** `(symbol, timestamp_at, feed)`.
* **Idempotency:** a partition is replaced whole in a single Delta commit (`replaceWhere` over the partitions present in the batch), never appended to, so a retry after a partial failure cannot duplicate rows and a reader never sees half a partition.
* **Pagination:** `limit=10000` + `next_page_token`, one request window per month.

## bronze / `dim_market_calendar`

From `GET /v2/calendar` (Trading API).

| Column | Type | Notes |
|---|---|---|
| `session_date` | `date32` | |
| `open_et`, `close_et` | `string` (`'HH:MM'`) | Early closes at 13:00 ET (Thanksgiving eve, 24 Dec) are real and frequent. Delta has no TIME type, so the string is the storage form; `read_market_calendar` parses it back to `datetime.time` for compute. |
| `session_minutes` | `int16` | 390 on a full session, 210 on an early close. |
| `is_half_day` | `bool` | `close_et < 16:00`. |
| `settlement_date` | `date32` | As returned by the API. |
| `ingested_at` | `timestamp[us, UTC]` | |

Without this table it is impossible to tell a minute with no trades from a closed market,
and that distinction changes how gaps get filled.

## bronze / `dim_corporate_actions`

From `GET /v1/corporate-actions`, normalised to one row per event.

| Column | Type | Notes |
|---|---|---|
| `symbol` | `string` | |
| `ex_date` | `date32` | |
| `type` | `string` | `split`, `cash_dividend`, `stock_dividend`, `merger`, `spin_off` |
| `ratio` | `decimal(18,10)` | Share multiplier: `4.0` for a 4:1 split, `1 + rate` for a stock dividend. |
| `cash_amount` | `decimal(18,6)` | Dividend amount per share. |
| `ingested_at` | `timestamp[us, UTC]` | |

AAPL in range: 4:1 split (2020-08-31) and quarterly dividends. `merger` and `spin_off` are
stored but produce no adjustment factor — the stored fields do not determine one.

## silver / `fact_bars_1m`

One row per minute of every regular session (09:30–15:59 ET), the cross product of
`dim_market_calendar` × session minutes with the bronze bars left-joined on.

Adds to the bronze columns:

| Column | Type | Definition |
|---|---|---|
| `timestamp_et_at` | `timestamp[us, America/New_York]` | `timestamp_at` in market time. Delta stores one timestamp type — a UTC instant — so the zone is a read-side view: `schemas.align_to_schema` re-attaches it, and Spark shows it in the session timezone. |
| `session_date` | `date32` | Session, not the timestamp's date (they differ pre/post-market). |
| `minute_index` | `int16` | 0–389, position within the session. |
| `adj_factor` | `decimal(18,10)` | Cumulative product of the factors of every action with `ex_date` **after** this session. |
| `split_factor` | `decimal(18,10)` | The split-only part of `adj_factor`; volume is divided by it. |
| `open_adj … close_adj`, `vwap_adj` | `decimal(18,6)` | Price × `adj_factor`. |
| `volume_adj` | `int64` | `volume ÷ split_factor`. |
| `is_imputed` | `bool` | The minute had no bar and was filled. |
| `is_half_day` | `bool` | From `dim_market_calendar`. |

**Adjustment factors.** A factor applies to sessions *strictly before* the ex-date; from the
ex-date on, the market already trades adjusted.

* split / stock dividend with share ratio `R` → prices × `1/R`, volume × `R`
* cash dividend of `d` with previous session close `C` → prices × `(1 − d/C)`

**Gap policy.** `close` is carried forward; `open = high = low = vwap = close`, `volume = 0`,
`trade_count = 0`, `is_imputed = true`. Prices are **never** interpolated linearly — that
invents movement that did not happen and contaminates every volatility estimate.

`is_imputed` is itself a feature (it marks illiquidity), so those rows stay in the table. It
is the *target* that must exclude them, since their return is zero by construction.

Sessions that begin before the first bar available in bronze are dropped whole, so every
session in the table is complete.

**Mandatory validations** (the job fails, it does not warn):

1. `low ≤ min(open, close) ≤ max(open, close) ≤ high`
2. `vwap` inside `[low, high]`
3. `volume > 0 ⟺ trade_count > 0`
4. 390 rows per full session, 210 per early close
5. no duplicates on `(symbol, timestamp_at)`
6. no 1-minute return with `|r| > 0.20` that no corporate action explains

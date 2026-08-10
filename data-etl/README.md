# data-etl

ETL pipelines for ingesting financial market data into a medallion lakehouse: Delta Lake
tables governed by Unity Catalog, plus TimescaleDB for serving.

## Overview

Pipelines follow a **medallion architecture**. Every table is an **external Delta table
registered in Unity Catalog**, so the same table is readable from pandas, from Spark
(`spark.table("lakehouse.bronze.fact_bars_raw")`) and from the Unity Catalog UI:

| Layer | Prefix | Description | Storage |
|---|---|---|---|
| Bronze | `i_` | Raw ingestion from external APIs | `lakehouse.bronze.*` (Delta) |
| Silver | `ii_` | Calendar alignment, adjustment, validation | `lakehouse.silver.*` (Delta) |
| Gold | `iii_` | Feature engineering, model-ready outputs | _(planned)_ |

## Directory layout

```
data-etl/
├── configs/                    # Spark configs, env settings, pipeline params
│   └── data_contracts/         # Field-level schema definitions
├── docs/                       # Data lineage and schema documentation
├── src/                        # Reusable Python modules
│   ├── extractions/            # API clients (Alpaca, Alpha Vantage)
│   ├── storage/                # Unity Catalog registration + Delta reads/writes
│   └── dwh/                    # Layer jobs
│       ├── schemas.py          # Versioned Arrow contracts
│       ├── bronze/             # origin → bronze
│       └── silver/             # bronze → silver
├── scripts/
│   └── dwh/
│       ├── i_origin_to_bronze/  # Raw ingestion notebooks (by data source)
│       ├── ii_bronze_to_silver/ # Cleaning notebooks (by data source)
│       └── iii_silver_to_gold/  # Feature engineering notebooks (planned)
└── tests/
    ├── unit/                   # Pure-python tests, no network
    ├── integration/            # Live API tests (need credentials)
    ├── infra/                  # Connectivity smoke tests
    └── data/                   # Data quality checks per layer
```

## Prerequisites

The Spark/Jupyter container managed by `quant-infrastructure/` must be running. See the [infrastructure README](../../quant-infrastructure/README.md) for setup instructions.

```sh
cd quant-infrastructure
docker compose up --build   # first time
docker compose start        # subsequent starts
```

Services after startup:
- Jupyter / Spark: http://localhost:8888
- Unity Catalog API: http://localhost:8081 · UI: http://localhost:3000
- Airflow: http://localhost:8080
- TimescaleDB: `localhost:5432`

## Data sources

### Alpaca Markets
Historical OHLCV bars, market calendar and corporate actions via the Alpaca REST API.
Requires `APCA-API-KEY-ID` and `APCA-API-SECRET-KEY` in `.env` (see `.env.example`).

### Alpha Vantage
Intraday and daily time series via the Alpha Vantage REST API. Requires `ALPHA_VANTAGE_API_KEY` in `.env`.

## Extraction module (`src/extractions/alpaca.py`)

`MarketData` is a thin, faithful wrapper over the Alpaca REST API: it paginates on
`next_page_token`, spaces requests to stay inside the 200 req/min budget, retries on 429
and 5xx, and returns tidy pandas frames.

```python
from src.extractions.alpaca import MarketData

market = MarketData(credentials={"APCA-API-KEY-ID": ..., "APCA-API-SECRET-KEY": ...})

market.get_bars(symbol="AAPL", timeframe="1Min", start="2016-01-01", end="2016-01-31", feed="sip")
market.get_calendar(start="2016-01-01", end="2016-12-31")
market.get_corporate_actions(symbol="AAPL", start="2020-01-01", end="2020-12-31")
```

## Market values 1-minute pipeline

Ingests every field Alpaca exposes for 1-minute bars and turns it into a gap-free,
adjusted, validated minute series. The field-level contract is
[`configs/data_contracts/alpaca_bars.md`](configs/data_contracts/alpaca_bars.md); the
authoritative schemas live in `src/dwh/schemas.py`.

| Unity Catalog table | Job |
|---|---|
| `lakehouse.bronze.fact_bars_raw` | `src.dwh.bronze.alpaca_bars.ingest_bars_raw` |
| `lakehouse.bronze.dim_market_calendar` | `src.dwh.bronze.alpaca_reference.ingest_market_calendar` |
| `lakehouse.bronze.dim_corporate_actions` | `src.dwh.bronze.alpaca_reference.ingest_corporate_actions` |
| `lakehouse.silver.fact_bars_1m` | `src.dwh.silver.bars_1m.run_bronze_to_silver` |

```python
from src.dwh.bronze import alpaca_bars, alpaca_reference
from src.dwh.silver import bars_1m

alpaca_reference.ingest_market_calendar(market, start="2016-01-01", end="2026-08-08")
alpaca_reference.ingest_corporate_actions(market, symbols="AAPL", start="2016-01-01", end="2026-08-08")
alpaca_bars.ingest_bars_raw(market, symbols="AAPL", start="2016-01-01", end="2026-08-08")
bars_1m.run_bronze_to_silver("AAPL")
```

Every job takes an optional `lakehouse=` handle (`src.storage.catalog.get_catalog()`);
without one it builds the default from the environment.

All jobs are idempotent and parameterised by `(symbol, date_range)`: a partition is
replaced whole in a single Delta commit rather than appended to, so a retry after a
partial failure cannot duplicate rows. Bronze always stores `adjustment=raw` — Alpaca
applies adjustments with the factors known *today*, so only raw data plus a versioned
corporate actions table makes the adjusted series reproducible.

Nothing outside `src/storage/catalog.py` builds a path: the job asks Unity Catalog where a
table lives, which is what keeps pandas, Spark and the catalog UI looking at one dataset.

For extractions that do not depend on Spark, the pipeline runs in the local virtual
environment in `env/`:

```sh
source data-etl/env/bin/activate
```

Local dependencies are pinned in `configs/local_requirements.txt` (pandas, pyarrow,
deltalake, polars, requests, python-dotenv, …). Point `UNITY_CATALOG_URI` at
`http://localhost:8081` and `LAKEHOUSE_ROOT` at a path you can write when running outside
the container — the location registered in the catalog must be reachable by every engine
that later reads the table.

## Tests

```sh
pytest data-etl/tests/unit          # no network, no credentials
pytest data-etl/tests/integration   # live Alpaca calls, needs credentials
```

Unit tests cover the extraction parsers, the layer jobs and the catalog writer end to end:
the Delta round trip is real, against a temporary warehouse root, and only the Unity
Catalog server is faked (`tests/unit/fakes.py`), so no service is needed. Tests that hit
the API skip themselves when `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` are unset.

## Running notebooks

Open Jupyter at http://localhost:8888. Notebooks are mounted from this repo into the container at `/home/jovyan/work`.

**Smoke tests — run these first when verifying connectivity:**

| Notebook | What it checks |
|---|---|
| `tests/infra/spark_catalog.ipynb` | Spark ↔ Delta Lake ↔ storage |
| `tests/infra/spark_postgresql.ipynb` | Spark ↔ PostgreSQL JDBC |

**Pipeline notebooks:**

| Notebook | Source | Description |
|---|---|---|
| `scripts/dwh/i_origin_to_bronze/alpaca/extract_aapl_1min_bars.ipynb` | Alpaca | AAPL 1-minute bars, calendar and corporate actions → `lakehouse.bronze.*` |
| `scripts/dwh/ii_bronze_to_silver/alpaca/build_bars_1m.ipynb` | — | bronze → `lakehouse.silver.fact_bars_1m`, gap-filled, adjusted, validated |
| `scripts/dwh/i_origin_to_bronze/extract_intraday_series.ipynb` | Alpha Vantage | Raw intraday request — exploratory, does not land in the catalog yet |

## Reading the tables from Spark

The jobs write with `deltalake`; Spark reads the same tables through the Unity Catalog
connector. Both `spark_catalog` and `lakehouse` must use `UCSingleCatalog`, or table
resolution silently falls back to Spark's session catalog:

```python
spark = (
    SparkSession.builder
    .appName("silver-bars-1m")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog")
    .config("spark.sql.catalog.spark_catalog.uri", os.environ["UNITY_CATALOG_URI"])
    .config("spark.sql.catalog.spark_catalog.token", "")
    .config("spark.sql.catalog.lakehouse", "io.unitycatalog.spark.UCSingleCatalog")
    .config("spark.sql.catalog.lakehouse.uri", os.environ["UNITY_CATALOG_URI"])
    .config("spark.sql.catalog.lakehouse.token", "")
    .getOrCreate()
)

spark.table("lakehouse.silver.fact_bars_1m").where("symbol = 'AAPL'").show()
```

The OSS connector supports **external** Delta tables only — which is what these jobs
register. Credentials are always injected via environment variables, never hardcoded.

## Key JVM dependencies

Passed via `PYSPARK_SUBMIT_ARGS` in `quant-infrastructure/docker-compose.yml`; no separate
install step. Only Spark readers need them — the ingestion jobs do not.

| Artifact | Version | Purpose |
|---|---|---|
| `io.delta:delta-spark_2.12` | 3.2.1 | Delta Lake (requires Spark 3.5.3) |
| `io.unitycatalog:unitycatalog-spark_2.12` | 0.2.0 | Unity Catalog Spark connector |
| `org.postgresql:postgresql` | 42.7.3 | JDBC driver for TimescaleDB |

## Naming conventions

**Tables:** `snake_case`, prefixed with `dim_` or `fact_` (Kimball), plural nouns — e.g. `fact_intraday_bars`.

**Columns:** `snake_case`; booleans prefixed `is_`/`has_`/`can_`; timestamps suffixed `_at`; dates `_date`; foreign keys `_id`.

**Code:** PEP 8 — classes `PascalCase`, functions/variables `snake_case`, constants `UPPER_SNAKE_CASE`.

# data-etl

PySpark + Jupyter ETL pipelines for ingesting financial market data into a medallion lakehouse architecture backed by MinIO (Delta Lake) and TimescaleDB.

## Overview

Pipelines follow a **medallion architecture**:

| Layer | Prefix | Description | Storage |
|---|---|---|---|
| Bronze | `i_` | Raw ingestion from external APIs | MinIO `s3a://bronze/...` (Delta Lake) |
| Silver | `ii_` | Cleaning, typing, deduplication | _(planned)_ |
| Gold | `iii_` | Feature engineering, model-ready outputs | _(planned)_ |

## Directory layout

```
data-etl/
├── configs/                    # Spark configs, env settings, pipeline params
│   └── data_contracts/         # Field-level schema definitions
├── docs/                       # Data lineage and schema documentation
├── src/                        # Reusable Python modules
│   └── extractions.py          # Extraction class hierarchy (Alpaca, Alpha Vantage)
├── scripts/
│   └── dwh/
│       ├── i_origin_to_bronze/ # Raw ingestion notebooks (by data source)
│       ├── ii_bronze_to_silver/ # Cleaning notebooks (planned)
│       └── iii_silver_to_gold/  # Feature engineering notebooks (planned)
└── tests/
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
- MinIO Console: http://localhost:9001
- TimescaleDB: `localhost:5432`

## Data sources

### Alpaca Markets
Real-time and historical OHLCV bars via the Alpaca REST API. Requires `ALPACA_API_KEY` in `.env`.

### Alpha Vantage
Intraday and daily time series via the Alpha Vantage REST API. Requires `ALPHA_VANTAGE_API_KEY` in `.env`.

## Extraction module (`src/extractions.py`)

The `Extractions` class inherits from source-specific classes (`AlpacaExtractor`, `AlphaVantageExtractor`) and exposes a unified interface for pulling stock market data:

```python
from src.extractions import Extractions

ext = Extractions()
df = ext.get_intraday_bars(symbol="AAPL", interval="5min")
```

For notebooks that do not depend on Spark, extractions can be run using the local virtual environment in `env/`:

```sh
source data-etl/env/bin/activate
```

Local dependencies are pinned in `configs/local_requirements.txt` (pandas, polars, requests, python-dotenv, …).

## Running notebooks

Open Jupyter at http://localhost:8888. Notebooks are mounted from this repo into the container at `/home/jovyan/work`.

**Smoke tests — run these first when verifying connectivity:**

| Notebook | What it checks |
|---|---|
| `tests/infra/spark_catalog.ipynb` | Spark ↔ Delta Lake ↔ MinIO |
| `tests/infra/spark_postgresql.ipynb` | Spark ↔ PostgreSQL JDBC |

**Bronze ingestion notebooks:**

| Notebook | Source | Description |
|---|---|---|
| `scripts/dwh/i_origin_to_bronze/extract_intraday_series.ipynb` | Alpha Vantage | Intraday OHLCV bars → MinIO bronze |

## SparkSession pattern

All notebooks that write to MinIO must configure S3A and Delta Lake:

```python
from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder
    .appName("my-pipeline")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

Credentials are always injected via environment variables — never hardcoded.

## Key JVM dependencies

Injected automatically via `PYSPARK_SUBMIT_ARGS` in `docker-compose.yml` — no manual install needed.

| Artifact | Version | Purpose |
|---|---|---|
| `io.delta:delta-core_2.12` | 2.4.0 | Delta Lake |
| `org.apache.hadoop:hadoop-aws` | 3.3.2 | S3A connector for MinIO |
| `org.postgresql:postgresql` | 42.7.3 | JDBC driver for TimescaleDB |

## Naming conventions

**Tables:** `snake_case`, prefixed with `dim_` or `fact_` (Kimball), plural nouns — e.g. `fact_intraday_bars`.

**Columns:** `snake_case`; booleans prefixed `is_`/`has_`/`can_`; timestamps suffixed `_at`; dates `_date`; foreign keys `_id`.

**Code:** PEP 8 — classes `PascalCase`, functions/variables `snake_case`, constants `UPPER_SNAKE_CASE`.

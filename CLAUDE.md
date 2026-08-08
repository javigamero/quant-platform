# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Prerequisites

All code runs inside the Spark/Jupyter container managed by the sibling `quant-infrastructure/` repo. The container must be running before executing any notebooks or ETL code. Infrastructure setup is documented in `../CLAUDE.md`.

## Running notebooks

All development happens through Jupyter at `http://localhost:8888`. Notebooks are mounted from this repo into the container at `/home/jovyan/work`.

**Infrastructure smoke tests** (run these first when setting up or debugging connectivity):
- `data-etl/tests/infra/spark_catalog.ipynb` — validates Spark ↔ Delta Lake ↔ MinIO
- `data-etl/tests/infra/spark_postgresql.ipynb` — validates Spark ↔ PostgreSQL JDBC

There is currently no `pytest` setup or CI. Tests are notebook-based; run them cell-by-cell in Jupyter.

## ETL scripts layout

Scripts are organized under `data-etl/scripts/dwh/` by DWH layer using a naming prefix:

| Prefix | Layer | Description |
|---|---|---|
| `i_` | origin → bronze | Raw ingestion from external APIs to the Parquet lake |
| `ii_` | bronze → silver | Calendar alignment, adjustment, validation |
| `iii_` | silver → gold | Feature engineering, model-ready outputs _(planned)_ |

Example: `data-etl/scripts/dwh/i_origin_to_bronze/alpaca/extract_aapl_1min_bars.ipynb`

New extraction notebooks go under `i_origin_to_bronze/`, grouped by data source (e.g. `alpha_vantage/`, `alpaca/`).

Notebooks stay thin: the logic lives in `data-etl/src/dwh/<layer>/`, which is unit-tested
under `data-etl/tests/unit/`. Schemas are versioned in `data-etl/src/dwh/schemas.py` and
documented in `data-etl/configs/data_contracts/`.

## Coding style Convention
Coding style is PEP8, please follow the next rules: 
* Classes: `PascalCase`
* Variables and functions: `snake_case`
* Constants: `UPPER_SNAKE_CASE`
* Private/Internal: leading underscore, 
* Modules & Packages: short, `snake_case`
* Booleans: `snake_case` and prefix with `is`, `has`, `can`, etc. 

## Tables/Columns naming convention: 
Tables and columns naming follow the next convention: 
* table names: 
  * `snake_case` 
  * prefix with `dim_`, `fact_` following Kimball dimensional modeling. 
  * plural nouns after prefix, for instance: `dim_products`
* columns names: 
  * `snake_case`
  * booleans named with prefix `is`, `has`, `can`, etc.
  * use `id` for primary key column 
  * use `_id` for foreign keys, for example: `user_id`. 
  * use `_at` for timetamps columns
  * use `_date` for date only columns
  * use `_time` for time only 
  * use `_year`. `_month`. `_day` for extracted parts.

## Shared modules

`shared/` contains cross-cutting code imported across all three layers:

```python
from shared.contracts import OHLCVBar, Signal, Position
from shared.config import MINIO_ENDPOINT, get_spark_session
from shared.utils import get_logger, retry
```

The three `shared/` modules (`contracts/`, `config/`, `utils/`) are currently **documentation stubs** — they contain comment blocks describing intended behaviour but no implemented code yet. Implement them before relying on their imports.

## Data contracts

Canonical schemas live in `shared/contracts/`. When a field changes shape (e.g. a new column added to OHLCV that ETL produces and models consume), update the contract and both sides in a single PR.

## Branch strategy

| Branch | Purpose |
|---|---|
| `main` | stable, deployable |
| `develop` | integration branch |
| `feature/<scope>/<name>` | e.g. `feature/etl/equities-silver` |
| `fix/<scope>/<name>` | bug fixes |
| `experiment/<name>` | exploratory / research work |

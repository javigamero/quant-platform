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
| `i_` | origin → bronze | Raw ingestion from external APIs to MinIO Delta tables |
| `ii_` | bronze → silver | Cleaning, typing, deduplication _(planned)_ |
| `iii_` | silver → gold | Feature engineering, model-ready outputs _(planned)_ |

Example: `data-etl/scripts/dwh/i_origin_to_bronze/extract_intraday_series.ipynb`

New extraction notebooks go under `i_origin_to_bronze/`, grouped by data source (e.g. `alpha_vantage/`, `alpaca/`).

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

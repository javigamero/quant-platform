# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Pre-requisites

All code runs inside the Spark/Jupyter container managed by the sibling `quant-infrastructure/` repo. The container must be running before executing any notebooks or ETL code. Infrastructure setup is documented in `../../CLAUDE.md`.

## Extractions
When running extractions, Claude may use local python virtual environment (`env/`) to execute python code that do not depends on spark.  
A module `src.extractions.alpaca` has been created to extract stocks values.

`MarketData` wraps the Alpaca REST API (`get_bars`, `get_calendar`, `get_corporate_actions`)
and only that — pagination, throttling and retries. Pipeline policy (feed, adjustment,
partitioning) lives in the layer jobs under `src/dwh/`, not in the client.

## Layer jobs

| Module | Produces |
|---|---|
| `src.dwh.bronze.alpaca_bars` | `bronze/fact_bars_raw` |
| `src.dwh.bronze.alpaca_reference` | `bronze/dim_market_calendar`, `bronze/dim_corporate_actions` |
| `src.dwh.silver.bars_1m` | `silver/fact_bars_1m` |
| `src.dwh.schemas` | Versioned Arrow contracts + `cast_to_schema` / `to_pandas` |
| `src.storage.lake` | Partitioned Parquet writes, idempotent per partition |

Decimal is the storage type for prices; `schemas.to_pandas` widens it to `float64` for
compute, because Arrow otherwise hands back `Decimal` objects that break arithmetic.

Run the tests with `env/bin/python -m pytest tests/unit`.
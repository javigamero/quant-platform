# quant-platform

Monorepo for all research and engineering work: data pipelines, quantitative
models, backtesting, and the shared library that ties them together.

Infrastructure (Docker Compose, database init, service config) lives in the
sibling `quant-infrastructure/` repository, which has its own lifecycle and
ownership.

## Repository layout

```
quant-platform/
├── data-etl/          # PySpark + Jupyter ETL pipelines (medallion architecture)
│   ├── configs/       # configuration files
│   ├── docs/          # documentation files to explain
│   ├── resources/     # resources definitions / DAGs, etc
│   ├── scripts/       # ingestion notebooks, organized by DWH layer
│   ├── src/           # reusable PySpark helpers
│   └── tests/         # etl test files
│
├── models/            # PyTorch model definitions, training loops, experiment tracking
│   ├── definitions/   # model architectures
│   ├── training/      # training scripts and configs
│   └── experiments/   # MLflow / experiment artefacts (gitignored)
│
├── backtesting/       # Strategy validation and performance reporting
│   ├── engine/        # core backtesting engine
│   ├── strategies/    # strategy implementations
│   └── reports/       # output reports (gitignored)
│
└── shared/            # Cross-cutting code imported by all three layers
    ├── contracts/     # canonical data schemas (OHLCV, Signal, Position, …)
    ├── config/        # environment loading, service URLs, constants
    └── utils/         # logging, date helpers, retry decorators, …
```

## Getting started

TODO Redefining... 

## Branch strategy

| Branch pattern        | Purpose                                  |
|-----------------------|------------------------------------------|
| `main`                | stable, deployable state                 |
| `develop`             | integration branch for ongoing work      |
| `feature/<scope>/<name>` | new features (e.g. `feature/etl/equities-silver`) |
| `fix/<scope>/<name>`  | bug fixes                                |
| `experiment/<name>`   | exploratory / research work              |
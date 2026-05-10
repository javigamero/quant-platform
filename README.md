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
│   ├── scripts/       # ingestion notebooks, organised by DWH layer
│   ├── validations/   # infrastructure smoke tests
│   ├── modules/       # reusable PySpark helpers
│   └── pipelines/     # pipeline definitions / DAGs
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

Spin up the infrastructure first:

```sh
cd ../quant-infrastructure
cp .env.example .env   # fill in credentials
docker compose up --build
```

Then work inside Jupyter at http://localhost:8888. The `quant-platform/`
directory is mounted into the Spark container at `/home/jovyan/work`.

## Branch strategy

| Branch pattern        | Purpose                                  |
|-----------------------|------------------------------------------|
| `main`                | stable, deployable state                 |
| `develop`             | integration branch for ongoing work      |
| `feature/<scope>/<name>` | new features (e.g. `feature/etl/equities-silver`) |
| `fix/<scope>/<name>`  | bug fixes                                |
| `experiment/<name>`   | exploratory / research work              |

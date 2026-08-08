"""Parquet data lake helpers.

Storage format is Parquet + Snappy: columnar, compresses numeric series well and is
read natively by both Spark and pandas.

Writes are **idempotent by partition**. A partition is always rewritten whole rather
than appended to, so a retry after a partial failure cannot duplicate rows — which is
the property the bronze layer's logical key `(symbol, timestamp_at, feed)` depends on.
"""

import os

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

_DEFAULT_LAKE_ROOT = "/data/lake"

COMPRESSION = "snappy"


def get_lake_root(lake_root: str=None) -> str:
    """Resolves the lake root: explicit argument, then `LAKE_ROOT`, then the mount default."""

    return lake_root or os.getenv("LAKE_ROOT", _DEFAULT_LAKE_ROOT)


def get_table_path(layer: str, table: str, lake_root: str=None) -> str:
    """Builds `<lake_root>/<layer>/<table>`, e.g. `/data/lake/bronze/fact_bars_raw`."""

    return os.path.join(get_lake_root(lake_root), layer, table)


def write_partitions(table: pa.Table, path: str, partition_columns: list) -> str:
    """Writes `table` as hive-partitioned Parquet, replacing every partition it touches.

    Partitions present on disk but absent from `table` are left untouched, so a monthly
    backfill can be resumed or replayed without rewriting the whole dataset.
    """

    if not partition_columns:
        return write_table(table, path)

    partitioning = ds.partitioning(
        pa.schema([table.schema.field(column) for column in partition_columns]),
        flavor="hive",
    )

    ds.write_dataset(
        table,
        base_dir=path,
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="delete_matching",
        file_options=ds.ParquetFileFormat().make_write_options(compression=COMPRESSION),
        basename_template="part-{i}.parquet",
    )

    return path


def write_table(table: pa.Table, path: str) -> str:
    """Overwrites a small, unpartitioned table (calendar, corporate actions) with a single file."""

    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, "part-0.parquet")
    pq.write_table(table, file_path, compression=COMPRESSION)

    return file_path


def read_table(path: str, columns: list=None, filters=None) -> pa.Table:
    """Reads a lake table back, returning an empty table when the path does not exist yet."""

    if not os.path.exists(path):
        return pa.table({})

    dataset = ds.dataset(path, format="parquet", partitioning="hive")

    return dataset.to_table(columns=columns, filter=filters)


def has_partition(path: str, partition_values: dict) -> bool:
    """Checks whether a hive partition directory already holds data files."""

    partition_path = path
    for column, value in partition_values.items():
        partition_path = os.path.join(partition_path, f"{column}={value}")

    if not os.path.isdir(partition_path):
        return False

    return any(name.endswith(".parquet") for name in os.listdir(partition_path))

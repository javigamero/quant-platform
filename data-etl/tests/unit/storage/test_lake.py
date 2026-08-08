import os

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pytest

from src.storage import lake

_PARTITION_COLUMNS = ["symbol", "year", "month"]


def make_table(symbol="AAPL", month="01", close=100.0, row_count=2) -> pa.Table:
    return pa.table({
        "symbol": [symbol] * row_count,
        "year": ["2016"] * row_count,
        "month": [month] * row_count,
        "close": [close] * row_count,
    })


class TestGetLakeRoot:

    def test_explicit_argument_wins(self, monkeypatch):
        monkeypatch.setenv("LAKE_ROOT", "/from/env")
        assert lake.get_lake_root("/explicit") == "/explicit"

    def test_falls_back_to_the_environment(self, monkeypatch):
        monkeypatch.setenv("LAKE_ROOT", "/from/env")
        assert lake.get_lake_root() == "/from/env"

    def test_falls_back_to_the_mount_default(self, monkeypatch):
        monkeypatch.delenv("LAKE_ROOT", raising=False)
        assert lake.get_lake_root() == "/data/lake"


class TestGetTablePath:

    def test_joins_root_layer_and_table(self):
        assert lake.get_table_path("bronze", "fact_bars_raw", "/lake") == "/lake/bronze/fact_bars_raw"


class TestWritePartitions:

    def test_creates_hive_style_directories(self, tmp_path):
        lake.write_partitions(make_table(), str(tmp_path), _PARTITION_COLUMNS)
        assert os.path.isdir(tmp_path / "symbol=AAPL" / "year=2016" / "month=01")

    def test_files_are_snappy_compressed(self, tmp_path):
        lake.write_partitions(make_table(), str(tmp_path), _PARTITION_COLUMNS)
        file_path = tmp_path / "symbol=AAPL" / "year=2016" / "month=01" / "part-0.parquet"

        assert pq.ParquetFile(file_path).metadata.row_group(0).column(0).compression == "SNAPPY"

    def test_rewriting_a_partition_replaces_its_rows(self, tmp_path):
        lake.write_partitions(make_table(close=100.0), str(tmp_path), _PARTITION_COLUMNS)
        lake.write_partitions(make_table(close=200.0), str(tmp_path), _PARTITION_COLUMNS)

        table = lake.read_table(str(tmp_path))

        assert table.num_rows == 2
        assert set(table.column("close").to_pylist()) == {200.0}

    def test_untouched_partitions_survive_a_rewrite(self, tmp_path):
        lake.write_partitions(make_table(month="01"), str(tmp_path), _PARTITION_COLUMNS)
        lake.write_partitions(make_table(month="02"), str(tmp_path), _PARTITION_COLUMNS)
        lake.write_partitions(make_table(month="01", close=200.0), str(tmp_path), _PARTITION_COLUMNS)

        assert lake.read_table(str(tmp_path)).num_rows == 4

    def test_other_symbols_are_left_alone(self, tmp_path):
        lake.write_partitions(make_table(symbol="AAPL"), str(tmp_path), _PARTITION_COLUMNS)
        lake.write_partitions(make_table(symbol="SPY"), str(tmp_path), _PARTITION_COLUMNS)

        symbols = lake.read_table(str(tmp_path)).column("symbol").to_pylist()

        assert set(symbols) == {"AAPL", "SPY"}

    def test_without_partition_columns_it_writes_a_single_file(self, tmp_path):
        lake.write_partitions(make_table(), str(tmp_path), [])
        assert os.path.isfile(tmp_path / "part-0.parquet")


class TestWriteTable:

    def test_overwrites_the_previous_content(self, tmp_path):
        lake.write_table(make_table(row_count=3), str(tmp_path))
        lake.write_table(make_table(row_count=1), str(tmp_path))

        assert lake.read_table(str(tmp_path)).num_rows == 1


class TestReadTable:

    def test_missing_path_returns_an_empty_table(self, tmp_path):
        assert lake.read_table(str(tmp_path / "absent")).num_columns == 0

    def test_partition_keys_are_restored_as_columns(self, tmp_path):
        lake.write_partitions(make_table(), str(tmp_path), _PARTITION_COLUMNS)
        table = lake.read_table(str(tmp_path))

        assert set(_PARTITION_COLUMNS) <= set(table.schema.names)

    def test_filters_are_pushed_down(self, tmp_path):
        lake.write_partitions(make_table(symbol="AAPL"), str(tmp_path), _PARTITION_COLUMNS)
        lake.write_partitions(make_table(symbol="SPY"), str(tmp_path), _PARTITION_COLUMNS)

        table = lake.read_table(str(tmp_path), filters=pa_ds.field("symbol") == "SPY")

        assert set(table.column("symbol").to_pylist()) == {"SPY"}

    def test_selects_only_the_requested_columns(self, tmp_path):
        lake.write_partitions(make_table(), str(tmp_path), _PARTITION_COLUMNS)
        table = lake.read_table(str(tmp_path), columns=["close"])

        assert table.schema.names == ["close"]


class TestHasPartition:

    def test_detects_a_written_partition(self, tmp_path):
        lake.write_partitions(make_table(), str(tmp_path), _PARTITION_COLUMNS)
        assert lake.has_partition(str(tmp_path), {"symbol": "AAPL", "year": "2016", "month": "01"})

    def test_reports_a_missing_partition(self, tmp_path):
        lake.write_partitions(make_table(), str(tmp_path), _PARTITION_COLUMNS)
        assert not lake.has_partition(str(tmp_path), {"symbol": "AAPL", "year": "2016", "month": "02"})

    def test_an_empty_directory_does_not_count_as_written(self, tmp_path):
        os.makedirs(tmp_path / "symbol=AAPL" / "year=2016" / "month=01")
        assert not lake.has_partition(str(tmp_path), {"symbol": "AAPL", "year": "2016", "month": "01"})

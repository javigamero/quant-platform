from datetime import date, datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from src.storage import catalog
from src.storage.catalog import UnityCatalog, UnityCatalogClient, UnityCatalogError
from tests.unit.fakes import FakeUnityCatalogClient

_PARTITION_COLUMNS = ["symbol", "year", "month"]

_SCHEMA = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("timestamp_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("close", pa.decimal128(18, 6)),
    pa.field("volume", pa.int64()),
    pa.field("year", pa.string(), nullable=False),
    pa.field("month", pa.string(), nullable=False),
])


def make_table(symbol="AAPL", month="01", close="100.000000", row_count=2) -> pa.Table:
    return pa.table({
        "symbol": [symbol] * row_count,
        "timestamp_at": [datetime(2016, 1, 4, 14, 30, tzinfo=timezone.utc)] * row_count,
        "close": pa.array([Decimal(close)] * row_count, type=pa.decimal128(18, 6)),
        "volume": [1000] * row_count,
        "year": ["2016"] * row_count,
        "month": [month] * row_count,
    }, schema=_SCHEMA)


class FakeResponse:

    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = str(self._payload)

    def json(self):
        return self._payload


class FakeSession:

    def __init__(self, responses=None):
        self.responses = list(responses or [FakeResponse()])
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})

        return self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]


class TestUnityCatalogClient:

    def test_builds_the_api_url(self):
        session = FakeSession()
        UnityCatalogClient(uri="http://uc:8080", session=session).get_table("lakehouse.bronze.t")

        assert session.calls[0]["url"] == "http://uc:8080/api/2.1/unity-catalog/tables/lakehouse.bronze.t"

    def test_a_trailing_slash_does_not_double_up(self):
        session = FakeSession()
        UnityCatalogClient(uri="http://uc:8080/", session=session).get_table("lakehouse.bronze.t")

        assert "8080/api" in session.calls[0]["url"]

    def test_an_unknown_table_reads_back_as_none(self):
        session = FakeSession([FakeResponse(status_code=404)])

        assert UnityCatalogClient(session=session).get_table("lakehouse.bronze.absent") is None

    def test_a_token_is_sent_as_a_bearer_header(self):
        session = FakeSession()
        UnityCatalogClient(token="secret", session=session).get_table("lakehouse.bronze.t")

        assert session.calls[0]["headers"]["Authorization"] == "Bearer secret"

    def test_no_token_sends_no_header(self, monkeypatch):
        monkeypatch.delenv("UNITY_CATALOG_TOKEN", raising=False)
        session = FakeSession()
        UnityCatalogClient(session=session).get_table("lakehouse.bronze.t")

        assert session.calls[0]["headers"] == {}

    def test_a_rejected_create_raises(self):
        # Unity Catalog answers a duplicate name with 400, not 409.
        session = FakeSession([FakeResponse(status_code=400, payload={"message": "already exists"})])

        with pytest.raises(UnityCatalogError, match="400"):
            UnityCatalogClient(session=session).create_table({"name": "t"})

    def test_deleting_an_absent_table_is_not_an_error(self):
        session = FakeSession([FakeResponse(status_code=404)])
        UnityCatalogClient(session=session).delete_table("lakehouse.bronze.absent")

    def test_lists_the_tables_of_a_schema(self):
        session = FakeSession([FakeResponse(payload={"tables": [{"name": "fact_bars_raw"}]})])
        tables = UnityCatalogClient(session=session).list_tables("lakehouse", "bronze")

        assert [table["name"] for table in tables] == ["fact_bars_raw"]
        assert session.calls[0]["params"] == {"catalog_name": "lakehouse", "schema_name": "bronze"}


class TestNaming:

    def test_table_names_are_three_level(self, lakehouse):
        assert lakehouse.get_table_name("bronze", "fact_bars_raw") == "lakehouse.bronze.fact_bars_raw"

    def test_storage_locations_are_absolute_file_uris(self, tmp_path):
        store = UnityCatalog(warehouse_root=str(tmp_path), client=FakeUnityCatalogClient())

        assert store.get_storage_location("bronze", "fact_bars_raw") == (
            f"file://{tmp_path}/bronze/fact_bars_raw"
        )

    def test_the_environment_configures_the_defaults(self, monkeypatch):
        monkeypatch.setenv("UNITY_CATALOG_NAME", "research")
        monkeypatch.setenv("LAKEHOUSE_ROOT", "/warehouse")
        store = UnityCatalog(client=FakeUnityCatalogClient())

        assert store.get_table_name("bronze", "t") == "research.bronze.t"
        assert store.get_storage_location("bronze", "t") == "file:///warehouse/bronze/t"

    def test_it_falls_back_to_the_mounted_default(self, monkeypatch):
        monkeypatch.delenv("LAKEHOUSE_ROOT", raising=False)
        store = UnityCatalog(client=FakeUnityCatalogClient())

        assert store.get_storage_location("bronze", "t") == "file:///data/lake/bronze/t"


class TestRegisterTable:

    def test_registers_an_external_delta_table(self, lakehouse):
        lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)
        registered = lakehouse.client.get_table("lakehouse.bronze.fact_bars_raw")

        assert registered["table_type"] == "EXTERNAL"
        assert registered["data_source_format"] == "DELTA"

    def test_registration_is_idempotent(self, lakehouse):
        lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)
        lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)

        assert len(lakehouse.client.list_tables("lakehouse", "bronze")) == 1

    def test_the_catalog_location_wins_over_the_convention(self, lakehouse):
        lakehouse.client.create_table({
            "name": "fact_bars_raw", "catalog_name": "lakehouse", "schema_name": "bronze",
            "storage_location": "file:///elsewhere/fact_bars_raw", "columns": [],
        })

        location = lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)

        assert location == "file:///elsewhere/fact_bars_raw"

    def test_partition_columns_are_flagged_in_order(self, lakehouse):
        lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)
        columns = lakehouse.client.get_table("lakehouse.bronze.fact_bars_raw")["columns"]

        assert {column["name"]: column["partition_index"]
                for column in columns if "partition_index" in column} == {"symbol": 0, "year": 1, "month": 2}

    def test_non_partition_columns_carry_no_index(self, lakehouse):
        lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)
        columns = lakehouse.client.get_table("lakehouse.bronze.fact_bars_raw")["columns"]

        assert "partition_index" not in [column for column in columns if column["name"] == "close"][0]


class TestBuildUcColumns:

    def test_decimals_carry_precision_and_scale(self):
        column = catalog.build_uc_columns(_SCHEMA)[2]

        assert (column["type_name"], column["type_text"]) == ("DECIMAL", "decimal(18,6)")
        assert (column["type_precision"], column["type_scale"]) == (18, 6)

    def test_positions_follow_the_schema_order(self):
        positions = [column["position"] for column in catalog.build_uc_columns(_SCHEMA)]

        assert positions == list(range(len(_SCHEMA)))

    def test_nullability_comes_from_the_contract(self):
        columns = {column["name"]: column["nullable"] for column in catalog.build_uc_columns(_SCHEMA)}

        assert columns["symbol"] is False
        assert columns["close"] is True

    @pytest.mark.parametrize("arrow_type, expected", [
        (pa.string(), ("STRING", "string")),
        (pa.int64(), ("LONG", "long")),
        (pa.int32(), ("INT", "int")),
        (pa.int16(), ("SHORT", "short")),
        (pa.bool_(), ("BOOLEAN", "boolean")),
        (pa.date32(), ("DATE", "date")),
        (pa.float64(), ("DOUBLE", "double")),
        (pa.timestamp("us", tz="UTC"), ("TIMESTAMP", "timestamp")),
        (pa.timestamp("us"), ("TIMESTAMP_NTZ", "timestamp_ntz")),
    ])
    def test_arrow_types_map_to_unity_catalog_types(self, arrow_type, expected):
        assert catalog.to_uc_type(arrow_type) == expected

    def test_an_unmappable_type_is_rejected(self):
        # Delta has no TIME type: the contract must not smuggle one in unnoticed.
        with pytest.raises(TypeError):
            catalog.to_uc_type(pa.time32("s"))


class TestWritePartitions:

    def test_the_first_write_creates_the_table(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)

        assert lakehouse.read_table("bronze", "fact_bars_raw").num_rows == 2

    def test_rewriting_a_partition_replaces_its_rows(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        lakehouse.write_partitions(
            make_table(close="200.000000"), "bronze", "fact_bars_raw", _PARTITION_COLUMNS
        )
        table = lakehouse.read_table("bronze", "fact_bars_raw")

        assert table.num_rows == 2
        assert {str(value) for value in table.column("close").to_pylist()} == {"200.000000"}

    def test_untouched_partitions_survive_a_rewrite(self, lakehouse):
        lakehouse.write_partitions(make_table(month="01"), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        lakehouse.write_partitions(make_table(month="02"), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        lakehouse.write_partitions(
            make_table(month="01", close="200.000000"), "bronze", "fact_bars_raw", _PARTITION_COLUMNS
        )

        assert lakehouse.read_table("bronze", "fact_bars_raw").num_rows == 4

    def test_other_symbols_are_left_alone(self, lakehouse):
        lakehouse.write_partitions(make_table(symbol="AAPL"), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        lakehouse.write_partitions(make_table(symbol="SPY"), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)

        symbols = lakehouse.read_table("bronze", "fact_bars_raw").column("symbol").to_pylist()

        assert set(symbols) == {"AAPL", "SPY"}

    def test_a_write_spanning_partitions_replaces_all_of_them(self, lakehouse):
        for month in ("01", "02"):
            lakehouse.write_partitions(make_table(month=month), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)

        both_months = pa.concat_tables([
            make_table(month="01", close="200.000000"), make_table(month="02", close="200.000000")
        ])
        lakehouse.write_partitions(both_months, "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        table = lakehouse.read_table("bronze", "fact_bars_raw")

        assert table.num_rows == 4
        assert {str(value) for value in table.column("close").to_pylist()} == {"200.000000"}

    def test_the_contract_types_survive_the_round_trip(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        schema = lakehouse.read_table("bronze", "fact_bars_raw").schema

        assert schema.field("close").type == pa.decimal128(18, 6)
        assert schema.field("timestamp_at").type == pa.timestamp("us", tz="UTC")

    def test_without_partition_columns_it_writes_the_whole_table(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "dim_calendar", [])

        assert lakehouse.read_table("bronze", "dim_calendar").num_rows == 2

    def test_an_empty_frame_registers_the_table_without_writing(self, lakehouse):
        lakehouse.write_partitions(_SCHEMA.empty_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)

        assert lakehouse.client.get_table("lakehouse.bronze.fact_bars_raw") is not None
        assert lakehouse.read_table("bronze", "fact_bars_raw").num_columns == 0


class TestWriteTable:

    def test_overwrites_the_previous_content(self, lakehouse):
        lakehouse.write_table(make_table(row_count=3), "bronze", "dim_calendar")
        lakehouse.write_table(make_table(row_count=1), "bronze", "dim_calendar")

        assert lakehouse.read_table("bronze", "dim_calendar").num_rows == 1

    def test_regional_timestamps_are_stored_as_utc_instants(self, lakehouse):
        et_schema = pa.schema([pa.field("timestamp_et_at", pa.timestamp("us", tz="America/New_York"))])
        data = pa.table(
            {"timestamp_et_at": [datetime(2016, 1, 4, 14, 30, tzinfo=timezone.utc)]}, schema=et_schema
        )
        lakehouse.write_table(data, "silver", "fact_bars_1m")
        written = lakehouse.read_table("silver", "fact_bars_1m")

        assert written.schema.field("timestamp_et_at").type.tz == "UTC"
        assert written.column("timestamp_et_at").to_pylist() == [
            datetime(2016, 1, 4, 14, 30, tzinfo=timezone.utc)
        ]


class TestReadTable:

    def test_an_unregistered_table_reads_back_empty(self, lakehouse):
        assert lakehouse.read_table("bronze", "absent").num_columns == 0

    def test_a_registered_but_unwritten_table_reads_back_empty(self, lakehouse):
        lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)

        assert lakehouse.read_table("bronze", "fact_bars_raw").num_columns == 0

    def test_partition_keys_are_restored_as_columns(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        table = lakehouse.read_table("bronze", "fact_bars_raw")

        assert set(_PARTITION_COLUMNS) <= set(table.schema.names)

    def test_filters_are_pushed_down(self, lakehouse):
        for symbol in ("AAPL", "SPY"):
            lakehouse.write_partitions(
                make_table(symbol=symbol), "bronze", "fact_bars_raw", _PARTITION_COLUMNS
            )
        table = lakehouse.read_table("bronze", "fact_bars_raw", filters=[("symbol", "=", "SPY")])

        assert set(table.column("symbol").to_pylist()) == {"SPY"}

    def test_selects_only_the_requested_columns(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)
        table = lakehouse.read_table("bronze", "fact_bars_raw", columns=["close"])

        assert table.schema.names == ["close"]


class TestHasPartition:

    def test_detects_a_written_partition(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)

        assert lakehouse.has_partition(
            "bronze", "fact_bars_raw", {"symbol": "AAPL", "year": "2016", "month": "01"}
        )

    def test_reports_a_missing_partition(self, lakehouse):
        lakehouse.write_partitions(make_table(), "bronze", "fact_bars_raw", _PARTITION_COLUMNS)

        assert not lakehouse.has_partition(
            "bronze", "fact_bars_raw", {"symbol": "AAPL", "year": "2016", "month": "02"}
        )

    def test_an_unregistered_table_has_no_partitions(self, lakehouse):
        assert not lakehouse.has_partition("bronze", "absent", {"symbol": "AAPL"})


class TestBuildPartitionPredicate:

    def test_scopes_the_overwrite_to_one_partition(self):
        predicate = catalog.build_partition_predicate(make_table(), _PARTITION_COLUMNS)

        assert predicate == "(symbol = 'AAPL' AND year = '2016' AND month = '01')"

    def test_every_partition_present_is_covered_once(self):
        data = pa.concat_tables([make_table(month="01"), make_table(month="02"), make_table(month="01")])
        predicate = catalog.build_partition_predicate(data, _PARTITION_COLUMNS)

        assert predicate == (
            "(symbol = 'AAPL' AND year = '2016' AND month = '01')"
            " OR (symbol = 'AAPL' AND year = '2016' AND month = '02')"
        )

    def test_quotes_are_escaped(self):
        assert catalog.to_sql_literal("BRK'A") == "'BRK''A'"

    def test_numbers_are_not_quoted(self):
        assert catalog.to_sql_literal(2016) == "2016"

    def test_dates_are_quoted(self):
        assert catalog.to_sql_literal(date(2016, 1, 4)) == "'2016-01-04'"


class TestDropTable:

    def test_unregisters_the_table(self, lakehouse):
        lakehouse.register_table("bronze", "fact_bars_raw", _SCHEMA, _PARTITION_COLUMNS)
        lakehouse.drop_table("bronze", "fact_bars_raw")

        assert lakehouse.client.get_table("lakehouse.bronze.fact_bars_raw") is None

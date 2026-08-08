from datetime import date, datetime, timezone

import pandas as pd
import pyarrow as pa
import pytest

from src.dwh import schemas
from src.dwh.bronze import alpaca_bars
from src.storage import lake

_INGESTED_AT = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)


def make_bars(symbol="AAPL", timestamps=("2016-01-04T14:30:00Z",)) -> pd.DataFrame:
    return pd.DataFrame({
        "symbol": symbol,
        "timestamp_at": pd.to_datetime(list(timestamps), utc=True),
        "open": 26.0,
        "high": 26.5,
        "low": 25.9,
        "close": 26.2,
        "volume": 1_000,
        "trade_count": 42,
        "vwap": 26.1,
    })


class FakeMarketData:
    """Records every `get_bars` call and replays a canned response."""

    def __init__(self, bars_by_window=None):
        self.calls = []
        self._bars_by_window = bars_by_window or {}

    def get_bars(self, **kwargs):
        self.calls.append(kwargs)
        window = kwargs["start"][:7]

        return self._bars_by_window.get(window, make_bars(timestamps=[f"{window}-04T14:30:00Z"]))


class TestIterMonthWindows:

    def test_single_month_yields_one_window(self):
        windows = list(alpaca_bars.iter_month_windows(date(2016, 1, 5), date(2016, 1, 20)))
        assert windows == [("2016", "01", date(2016, 1, 5), date(2016, 1, 20))]

    def test_windows_are_clipped_to_the_requested_bounds(self):
        windows = list(alpaca_bars.iter_month_windows(date(2016, 1, 15), date(2016, 3, 10)))
        assert [window[:2] for window in windows] == [("2016", "01"), ("2016", "02"), ("2016", "03")]
        assert windows[0][2] == date(2016, 1, 15)
        assert windows[0][3] == date(2016, 1, 31)
        assert windows[-1][3] == date(2016, 3, 10)

    def test_windows_cross_the_year_boundary(self):
        windows = list(alpaca_bars.iter_month_windows(date(2016, 12, 1), date(2017, 1, 31)))
        assert [window[:2] for window in windows] == [("2016", "12"), ("2017", "01")]

    def test_inverted_range_yields_nothing(self):
        assert list(alpaca_bars.iter_month_windows(date(2016, 3, 1), date(2016, 1, 1))) == []

    def test_month_is_zero_padded(self):
        windows = list(alpaca_bars.iter_month_windows(date(2016, 3, 1), date(2016, 3, 1)))
        assert windows[0][1] == "03"


class TestBuildBarsRaw:

    @pytest.fixture
    def table(self):
        return alpaca_bars.build_bars_raw(
            make_bars(), feed="sip", adjustment="raw", currency="USD", ingested_at=_INGESTED_AT
        )

    def test_matches_the_contract_schema(self, table):
        assert table.schema == schemas.BARS_RAW_SCHEMA

    def test_prices_are_stored_as_decimals(self, table):
        assert table.schema.field("close").type == pa.decimal128(18, 6)

    def test_request_parameters_are_recorded_per_row(self, table):
        row = table.to_pylist()[0]
        assert row["feed"] == "sip"
        assert row["adjustment"] == "raw"
        assert row["currency"] == "USD"

    def test_ingestion_clock_is_recorded(self, table):
        assert table.to_pylist()[0]["ingested_at"] == _INGESTED_AT

    def test_partition_keys_come_from_the_utc_timestamp(self, table):
        row = table.to_pylist()[0]
        assert (row["year"], row["month"]) == ("2016", "01")

    def test_naive_ingestion_clock_is_treated_as_utc(self):
        table = alpaca_bars.build_bars_raw(
            make_bars(), feed="sip", adjustment="raw", currency="USD", ingested_at=datetime(2026, 8, 8, 12, 0)
        )
        assert table.to_pylist()[0]["ingested_at"] == _INGESTED_AT


class TestIngestBarsRaw:

    def test_writes_one_partition_per_month(self, tmp_path):
        market = FakeMarketData()
        alpaca_bars.ingest_bars_raw(
            market, symbols="AAPL", start="2016-01-01", end="2016-03-31", lake_root=str(tmp_path)
        )

        path = lake.get_table_path(schemas.BRONZE_LAYER, schemas.BARS_RAW_TABLE, str(tmp_path))
        assert lake.has_partition(path, {"symbol": "AAPL", "year": "2016", "month": "01"})
        assert lake.has_partition(path, {"symbol": "AAPL", "year": "2016", "month": "03"})

    def test_requests_the_fixed_pipeline_parameters(self, tmp_path):
        market = FakeMarketData()
        alpaca_bars.ingest_bars_raw(
            market, symbols="AAPL", start="2016-01-01", end="2016-01-31", lake_root=str(tmp_path)
        )

        call = market.calls[0]
        assert call["timeframe"] == "1Min"
        assert call["feed"] == "sip"
        assert call["adjustment"] == "raw"
        assert call["limit"] == alpaca_bars.PAGE_LIMIT

    def test_request_window_covers_the_whole_month(self, tmp_path):
        market = FakeMarketData()
        alpaca_bars.ingest_bars_raw(
            market, symbols="AAPL", start="2016-02-01", end="2016-02-29", lake_root=str(tmp_path)
        )

        call = market.calls[0]
        assert call["start"] == "2016-02-01T00:00:00Z"
        assert call["end"] == "2016-02-29T23:59:59.999Z"

    def test_summary_reports_rows_written_per_month(self, tmp_path):
        market = FakeMarketData()
        summary = alpaca_bars.ingest_bars_raw(
            market, symbols="AAPL", start="2016-01-01", end="2016-02-29", lake_root=str(tmp_path)
        )

        assert summary["row_count"].tolist() == [1, 1]

    def test_rerunning_does_not_duplicate_rows(self, tmp_path):
        market = FakeMarketData()
        for _ in range(2):
            alpaca_bars.ingest_bars_raw(
                market, symbols="AAPL", start="2016-01-01", end="2016-01-31", lake_root=str(tmp_path)
            )

        assert len(alpaca_bars.read_bars_raw(symbol="AAPL", lake_root=str(tmp_path))) == 1

    def test_existing_months_are_skipped_when_not_overwriting(self, tmp_path):
        market = FakeMarketData()
        alpaca_bars.ingest_bars_raw(
            market, symbols="AAPL", start="2016-01-01", end="2016-01-31", lake_root=str(tmp_path)
        )
        summary = alpaca_bars.ingest_bars_raw(
            market, symbols="AAPL", start="2016-01-01", end="2016-01-31",
            lake_root=str(tmp_path), is_overwrite=False,
        )

        assert summary["is_skipped"].tolist() == [True]
        assert len(market.calls) == 1

    def test_empty_response_writes_nothing(self, tmp_path):
        market = FakeMarketData({"2016-01": pd.DataFrame(columns=["symbol", "timestamp_at"])})
        summary = alpaca_bars.ingest_bars_raw(
            market, symbols="AAPL", start="2016-01-01", end="2016-01-31", lake_root=str(tmp_path)
        )

        assert summary["row_count"].tolist() == [0]
        assert alpaca_bars.read_bars_raw(lake_root=str(tmp_path)).empty


class TestReadBarsRaw:

    def test_missing_table_returns_an_empty_frame(self, tmp_path):
        bars = alpaca_bars.read_bars_raw(lake_root=str(tmp_path))
        assert bars.empty
        assert list(bars.columns) == list(schemas.BARS_RAW_SCHEMA.names)

    def test_decimals_are_widened_to_float(self, tmp_path):
        alpaca_bars.ingest_bars_raw(
            FakeMarketData(), symbols="AAPL", start="2016-01-01", end="2016-01-31", lake_root=str(tmp_path)
        )
        bars = alpaca_bars.read_bars_raw(symbol="AAPL", lake_root=str(tmp_path))

        assert pd.api.types.is_float_dtype(bars["close"])

    def test_filters_by_symbol(self, tmp_path):
        path = lake.get_table_path(schemas.BRONZE_LAYER, schemas.BARS_RAW_TABLE, str(tmp_path))
        for symbol in ("AAPL", "SPY"):
            table = alpaca_bars.build_bars_raw(
                make_bars(symbol=symbol), feed="sip", adjustment="raw", currency="USD"
            )
            lake.write_partitions(table, path, schemas.PARTITION_COLUMNS)

        assert alpaca_bars.read_bars_raw(symbol="SPY", lake_root=str(tmp_path))["symbol"].tolist() == ["SPY"]

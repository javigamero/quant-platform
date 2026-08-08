from datetime import date, datetime, time, timezone

import numpy as np
import pandas as pd
import pytest

from src.dwh import schemas
from src.dwh.bronze import alpaca_reference

_INGESTED_AT = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)

_CALENDAR = pd.DataFrame([
    {"session_date": "2016-01-04", "open_et": "09:30", "close_et": "16:00", "settlement_date": "2016-01-06"},
    {"session_date": "2015-11-27", "open_et": "09:30", "close_et": "13:00", "settlement_date": "2015-12-01"},
])

_ACTIONS = pd.DataFrame([
    {"symbol": "AAPL", "ex_date": "2020-08-31", "type": "split", "ratio": 4.0, "cash_amount": np.nan},
    {"symbol": "AAPL", "ex_date": "2020-08-07", "type": "cash_dividend", "ratio": np.nan, "cash_amount": 0.82},
])


class FakeMarketData:

    def __init__(self, calendar=None, actions=None):
        self.calendar_calls = []
        self.action_calls = []
        self._calendar = _CALENDAR if calendar is None else calendar
        self._actions = _ACTIONS if actions is None else actions

    def get_calendar(self, start=None, end=None):
        self.calendar_calls.append((start, end))
        return self._calendar

    def get_corporate_actions(self, symbol, start=None, end=None):
        self.action_calls.append((symbol, start, end))
        return self._actions


class TestParseEtTime:

    def test_parses_the_api_format(self):
        assert alpaca_reference.parse_et_time("09:30") == time(9, 30)

    def test_parses_a_seconds_precision_value(self):
        assert alpaca_reference.parse_et_time("13:00:00") == time(13, 0)

    def test_passes_through_an_existing_time(self):
        assert alpaca_reference.parse_et_time(time(16, 0)) == time(16, 0)


class TestBuildMarketCalendar:

    @pytest.fixture
    def table(self):
        return alpaca_reference.build_market_calendar(_CALENDAR, ingested_at=_INGESTED_AT)

    def test_matches_the_contract_schema(self, table):
        assert table.schema == schemas.MARKET_CALENDAR_SCHEMA

    def test_session_length_of_a_full_day_is_390_minutes(self, table):
        row = _row_for(table, date(2016, 1, 4))
        assert row["session_minutes"] == 390

    def test_session_length_of_an_early_close_is_210_minutes(self, table):
        row = _row_for(table, date(2015, 11, 27))
        assert row["session_minutes"] == 210

    def test_early_closes_are_flagged(self, table):
        assert _row_for(table, date(2015, 11, 27))["is_half_day"] is True

    def test_regular_sessions_are_not_flagged(self, table):
        assert _row_for(table, date(2016, 1, 4))["is_half_day"] is False

    def test_times_are_stored_as_time_of_day(self, table):
        assert _row_for(table, date(2016, 1, 4))["open_et"] == time(9, 30)

    def test_missing_settlement_dates_are_tolerated(self):
        calendar = _CALENDAR.drop(columns=["settlement_date"]).assign(settlement_date=None)
        table = alpaca_reference.build_market_calendar(calendar, ingested_at=_INGESTED_AT)

        assert table.column("settlement_date").null_count == table.num_rows


class TestBuildCorporateActions:

    @pytest.fixture
    def table(self):
        return alpaca_reference.build_corporate_actions(_ACTIONS, ingested_at=_INGESTED_AT)

    def test_matches_the_contract_schema(self, table):
        assert table.schema == schemas.CORPORATE_ACTIONS_SCHEMA

    def test_ex_dates_are_stored_as_dates(self, table):
        assert table.to_pylist()[0]["ex_date"] == date(2020, 8, 31)

    def test_split_ratio_is_preserved_at_factor_precision(self, table):
        split = [row for row in table.to_pylist() if row["type"] == "split"][0]
        assert float(split["ratio"]) == pytest.approx(4.0)

    def test_dividend_amount_is_preserved(self, table):
        dividend = [row for row in table.to_pylist() if row["type"] == "cash_dividend"][0]
        assert float(dividend["cash_amount"]) == pytest.approx(0.82)


class TestIngestMarketCalendar:

    def test_writes_and_reads_back_the_calendar(self, tmp_path):
        alpaca_reference.ingest_market_calendar(
            FakeMarketData(), start="2015-11-01", end="2016-01-31", lake_root=str(tmp_path)
        )
        calendar = alpaca_reference.read_market_calendar(lake_root=str(tmp_path))

        assert len(calendar) == 2

    def test_rerunning_replaces_instead_of_appending(self, tmp_path):
        market = FakeMarketData()
        for _ in range(2):
            alpaca_reference.ingest_market_calendar(
                market, start="2015-11-01", end="2016-01-31", lake_root=str(tmp_path)
            )

        assert len(alpaca_reference.read_market_calendar(lake_root=str(tmp_path))) == 2

    def test_dates_are_passed_to_the_api_as_iso_days(self, tmp_path):
        market = FakeMarketData()
        alpaca_reference.ingest_market_calendar(
            market, start=date(2015, 11, 1), end="2016-01-31", lake_root=str(tmp_path)
        )

        assert market.calendar_calls == [("2015-11-01", "2016-01-31")]

    def test_missing_table_reads_back_empty(self, tmp_path):
        calendar = alpaca_reference.read_market_calendar(lake_root=str(tmp_path))
        assert calendar.empty
        assert list(calendar.columns) == list(schemas.MARKET_CALENDAR_SCHEMA.names)


class TestIngestCorporateActions:

    def test_walks_the_range_in_yearly_windows(self, tmp_path):
        market = FakeMarketData(actions=pd.DataFrame(columns=_ACTIONS.columns))
        alpaca_reference.ingest_corporate_actions(
            market, symbols="AAPL", start="2016-06-01", end="2018-03-01", lake_root=str(tmp_path)
        )

        assert [call[1:] for call in market.action_calls] == [
            ("2016-06-01", "2016-12-31"),
            ("2017-01-01", "2017-12-31"),
            ("2018-01-01", "2018-03-01"),
        ]

    def test_repeated_actions_across_windows_are_deduplicated(self, tmp_path):
        market = FakeMarketData()
        alpaca_reference.ingest_corporate_actions(
            market, symbols="AAPL", start="2016-01-01", end="2018-01-01", lake_root=str(tmp_path)
        )

        assert len(alpaca_reference.read_corporate_actions(lake_root=str(tmp_path))) == 2

    def test_no_actions_writes_nothing(self, tmp_path):
        market = FakeMarketData(actions=pd.DataFrame(columns=_ACTIONS.columns))
        actions = alpaca_reference.ingest_corporate_actions(
            market, symbols="AAPL", start="2016-01-01", end="2016-12-31", lake_root=str(tmp_path)
        )

        assert actions.empty
        assert alpaca_reference.read_corporate_actions(lake_root=str(tmp_path)).empty

    def test_decimals_are_widened_to_float_on_read(self, tmp_path):
        alpaca_reference.ingest_corporate_actions(
            FakeMarketData(), symbols="AAPL", start="2020-01-01", end="2020-12-31", lake_root=str(tmp_path)
        )
        actions = alpaca_reference.read_corporate_actions(lake_root=str(tmp_path))

        assert pd.api.types.is_float_dtype(actions["ratio"])


def _row_for(table, session_date):
    return [row for row in table.to_pylist() if row["session_date"] == session_date][0]

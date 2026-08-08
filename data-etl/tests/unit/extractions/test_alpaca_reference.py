import os
from datetime import time

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.dwh.bronze.alpaca_reference import parse_et_time
from src.extractions.alpaca import MarketData

_CREDENTIALS = {
    "APCA-API-KEY-ID": os.getenv("APCA-API-KEY-ID"),
    "APCA-API-SECRET-KEY": os.getenv("APCA-API-SECRET-KEY"),
}

_CALENDAR_COLUMNS = ["session_date", "open_et", "close_et", "settlement_date"]
_CORPORATE_ACTION_COLUMNS = ["symbol", "ex_date", "type", "ratio", "cash_amount"]

_RAW_CALENDAR = [
    {"date": "2016-01-04", "open": "09:30", "close": "16:00", "settlement_date": "2016-01-06"},
    {"date": "2015-11-27", "open": "09:30", "close": "13:00", "settlement_date": "2015-12-01"},
]

_RAW_CORPORATE_ACTIONS = {
    "forward_splits": [
        {"symbol": "AAPL", "ex_date": "2020-08-31", "new_rate": 4, "old_rate": 1},
    ],
    "cash_dividends": [
        {"symbol": "AAPL", "ex_date": "2020-08-07", "rate": 0.82},
    ],
    "stock_dividends": [
        {"symbol": "AAPL", "ex_date": "2019-05-10", "rate": 0.05},
    ],
}


@pytest.fixture(scope="module")
def market():
    return MarketData(credentials=_CREDENTIALS)


class TestParseCalendar:

    def test_empty_input_returns_empty_dataframe(self, market):
        df = market._parse_calendar([])
        assert df.empty
        assert list(df.columns) == _CALENDAR_COLUMNS

    def test_output_has_expected_columns(self, market):
        df = market._parse_calendar(_RAW_CALENDAR)
        assert list(df.columns) == _CALENDAR_COLUMNS

    def test_sessions_are_sorted_by_date(self, market):
        df = market._parse_calendar(_RAW_CALENDAR)
        assert df["session_date"].tolist() == ["2015-11-27", "2016-01-04"]

    def test_missing_settlement_date_is_added_as_null(self, market):
        df = market._parse_calendar([{"date": "2016-01-04", "open": "09:30", "close": "16:00"}])
        assert df["settlement_date"].isna().all()


class TestParseCorporateActions:

    def test_empty_input_returns_empty_dataframe(self, market):
        df = market._parse_corporate_actions({})
        assert df.empty
        assert list(df.columns) == _CORPORATE_ACTION_COLUMNS

    def test_output_has_expected_columns(self, market):
        df = market._parse_corporate_actions(_RAW_CORPORATE_ACTIONS)
        assert list(df.columns) == _CORPORATE_ACTION_COLUMNS

    def test_collections_are_mapped_to_normalised_types(self, market):
        df = market._parse_corporate_actions(_RAW_CORPORATE_ACTIONS)
        assert sorted(df["type"].unique()) == ["cash_dividend", "split", "stock_dividend"]

    def test_forward_split_ratio_is_new_over_old_rate(self, market):
        df = market._parse_corporate_actions(_RAW_CORPORATE_ACTIONS)
        split = df[df["type"] == "split"].iloc[0]
        assert split["ratio"] == pytest.approx(4.0)
        assert split["ex_date"] == "2020-08-31"

    def test_reverse_split_ratio_is_below_one(self, market):
        df = market._parse_corporate_actions(
            {"reverse_splits": [{"symbol": "AAPL", "ex_date": "2020-08-31", "new_rate": 1, "old_rate": 10}]}
        )
        assert df.iloc[0]["ratio"] == pytest.approx(0.1)

    def test_cash_dividend_carries_amount_and_no_ratio(self, market):
        df = market._parse_corporate_actions(_RAW_CORPORATE_ACTIONS)
        dividend = df[df["type"] == "cash_dividend"].iloc[0]
        assert dividend["cash_amount"] == pytest.approx(0.82)
        assert dividend["ratio"] != dividend["ratio"]  # NaN

    def test_stock_dividend_ratio_is_one_plus_rate(self, market):
        df = market._parse_corporate_actions(_RAW_CORPORATE_ACTIONS)
        stock_dividend = df[df["type"] == "stock_dividend"].iloc[0]
        assert stock_dividend["ratio"] == pytest.approx(1.05)

    def test_split_with_zero_old_rate_yields_no_ratio(self, market):
        df = market._parse_corporate_actions(
            {"forward_splits": [{"symbol": "AAPL", "ex_date": "2020-08-31", "new_rate": 4, "old_rate": 0}]}
        )
        assert df.iloc[0]["ratio"] != df.iloc[0]["ratio"]  # NaN

    def test_unknown_collection_keeps_its_name_as_type(self, market):
        df = market._parse_corporate_actions(
            {"redemptions": [{"symbol": "AAPL", "ex_date": "2021-01-04"}]}
        )
        assert df.iloc[0]["type"] == "redemptions"


@pytest.mark.skipif(
    not all(_CREDENTIALS.values()),
    reason="Alpaca credentials not set — set APCA-API-KEY-ID and APCA-API-SECRET-KEY to run",
)
class TestGetCalendar:
    """Live API tests — require valid APCA credentials and network access."""

    @pytest.fixture(scope="class")
    def calendar(self):
        market = MarketData(credentials=_CREDENTIALS)
        return market.get_calendar(start="2015-11-23", end="2015-11-30")

    def test_returns_the_expected_sessions(self, calendar):
        assert calendar["session_date"].tolist() == [
            "2015-11-23", "2015-11-24", "2015-11-25", "2015-11-27", "2015-11-30"
        ]

    def test_detects_the_thanksgiving_eve_early_close(self, calendar):
        early_close = calendar[calendar["session_date"] == "2015-11-27"].iloc[0]
        assert parse_et_time(early_close["close_et"]) == time(13, 0)

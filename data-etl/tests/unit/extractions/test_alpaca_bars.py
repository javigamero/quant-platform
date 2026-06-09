import os
from datetime import timezone

import pandas as pd
import pytest
from dotenv import load_dotenv

load_dotenv()

from src.extractions.alpaca import Authenticator, MarketData

_CREDENTIALS = {
    "APCA-API-KEY-ID": os.getenv("APCA-API-KEY-ID"),
    "APCA-API-SECRET-KEY": os.getenv("APCA-API-SECRET-KEY"),
}

_EXPECTED_COLUMNS = ["timestamp_at", "open", "high", "low", "close", "volume", "trade_count", "vwap"]

_RAW_BAR = {
    "t": "2025-01-02T14:00:00Z",
    "o": 249.1,
    "h": 249.25,
    "l": 244.8,
    "c": 246.85,
    "v": 9049202,
    "n": 128106,
    "vw": 246.47,
}


class TestAuthenticator:

    def test_api_key_mode_accepts_valid_credentials(self):
        auth = Authenticator(credentials=_CREDENTIALS, auth_mode="api-key")
        assert auth.auth_mode == "api-key"

    def test_raises_on_unsupported_auth_mode(self):
        with pytest.raises(ValueError, match="Unsupported auth_mode"):
            Authenticator(credentials=_CREDENTIALS, auth_mode="invalid")

    def test_raises_on_missing_api_key_credentials(self):
        with pytest.raises(ValueError, match="Missing required credentials"):
            Authenticator(credentials={}, auth_mode="api-key")

    def test_get_headers_returns_credentials_dict(self):
        auth = Authenticator(credentials=_CREDENTIALS, auth_mode="api-key")
        assert auth.get_headers() == _CREDENTIALS

    def test_client_mode_get_headers_raises_not_implemented(self):
        auth = Authenticator(credentials={}, auth_mode="client")
        with pytest.raises(NotImplementedError):
            auth.get_headers()


class TestParseBars:

    @pytest.fixture(scope="class")
    def market(self):
        return MarketData(credentials=_CREDENTIALS)

    def test_empty_input_returns_empty_dataframe(self, market):
        df = market._parse_bars([])
        assert df.empty
        assert list(df.columns) == _EXPECTED_COLUMNS

    def test_output_has_expected_columns(self, market):
        df = market._parse_bars([_RAW_BAR])
        assert list(df.columns) == _EXPECTED_COLUMNS

    def test_timestamp_is_utc_aware(self, market):
        df = market._parse_bars([_RAW_BAR])
        assert df["timestamp_at"].dt.tz == timezone.utc

    def test_single_bar_values_are_mapped_correctly(self, market):
        df = market._parse_bars([_RAW_BAR])
        row = df.iloc[0]
        assert row["open"] == pytest.approx(249.1)
        assert row["high"] == pytest.approx(249.25)
        assert row["low"] == pytest.approx(244.8)
        assert row["close"] == pytest.approx(246.85)
        assert row["volume"] == 9049202
        assert row["trade_count"] == 128106
        assert row["vwap"] == pytest.approx(246.47)


class TestGetBars:
    """Live API tests — require valid APCA credentials and network access."""

    _SYMBOL = "AAPL"
    _START = "2025-01-01"
    _END = "2025-01-02"
    _TIMEFRAME = "1H"
    _EXPECTED_BAR_COUNT = 17

    @pytest.fixture(scope="class")
    def bars(self):
        market = MarketData(credentials=_CREDENTIALS)
        return market.get_bars(
            symbol=self._SYMBOL,
            timeframe=self._TIMEFRAME,
            start=self._START,
            end=self._END,
        )

    def test_returns_dataframe(self, bars):
        assert isinstance(bars, pd.DataFrame)

    def test_has_expected_columns(self, bars):
        assert list(bars.columns) == _EXPECTED_COLUMNS

    def test_returns_expected_bar_count(self, bars):
        assert len(bars) == self._EXPECTED_BAR_COUNT

    def test_timestamps_are_utc_aware(self, bars):
        assert bars["timestamp_at"].dt.tz == timezone.utc

    def test_timestamps_are_sorted_ascending(self, bars):
        assert bars["timestamp_at"].is_monotonic_increasing

    def test_first_bar_timestamp(self, bars):
        assert bars.iloc[0]["timestamp_at"] == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")

    def test_first_bar_ohlcv_values(self, bars):
        first = bars.iloc[0]
        assert first["open"] == pytest.approx(250.4216)
        assert first["high"] == pytest.approx(250.450)
        assert first["low"] == pytest.approx(250.410)
        assert first["close"] == pytest.approx(250.410)
        assert first["volume"] == 7042
        assert first["trade_count"] == 130

    def test_ohlc_columns_are_float(self, bars):
        for col in ["open", "high", "low", "close", "vwap"]:
            assert pd.api.types.is_float_dtype(bars[col])

    def test_volume_and_trade_count_are_non_negative(self, bars):
        assert (bars["volume"] >= 0).all()
        assert (bars["trade_count"] >= 0).all()

    def test_high_is_always_gte_low(self, bars):
        assert (bars["high"] >= bars["low"]).all()

    def test_last_bar_timestamp(self, bars):
        assert bars.iloc[-1]["timestamp_at"] == pd.Timestamp("2025-01-03 00:00:00", tz="UTC")

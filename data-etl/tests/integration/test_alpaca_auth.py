import os

import pytest
from dotenv import load_dotenv

load_dotenv()

from src.extractions.alpaca import MarketData

_credentials = {
    "APCA-API-KEY-ID": os.getenv("APCA-API-KEY-ID"),
    "APCA-API-SECRET-KEY": os.getenv("APCA-API-SECRET-KEY"),
}

pytestmark = pytest.mark.skipif(
    not all(_credentials.values()),
    reason="Alpaca credentials not set — set APCA-API-KEY-ID and APCA-API-SECRET-KEY to run",
)


class TestCredentials:

    def test_api_key_authenticates_successfully(self):
        market = MarketData(credentials=_credentials)
        df = market.get_bars(symbol="AAPL", timeframe="1D", start="2025-01-02", end="2025-01-02")
        assert not df.empty

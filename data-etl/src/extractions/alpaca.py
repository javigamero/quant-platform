import os
import time

import pandas as pd
import requests


class Authenticator:
    """Base class for handling authentication with Alpaca API.

    Parameters
    ----------
    * auth_mode : str
        The authentication mode to use. Currently supports:
        - 'client': Placeholder for client-based authentication (not implemented)
        - 'api-key': Uses API key and secret from environment variables

    * credentials : dict
        A dictionary of credentials required for the chosen authentication mode.
        For auth_mode 'api-key', this should include:
            - 'APCA-API-KEY-ID': Your Alpaca API key ID
            - 'APCA-API-SECRET-KEY': Your Alpaca API secret key
        For auth_mode 'client', the required credentials are not defined yet as
        this mode is not implemented.
    """

    def __init__(self,  credentials: dict, auth_mode: str='client'):

        if auth_mode not in ['client', 'api-key']:
            raise ValueError(f"Unsupported auth_mode '{auth_mode}'. Supported modes are 'client' and 'api-key'.")
        else:
            self.auth_mode = auth_mode

        if self.auth_mode == 'api-key':
            required_keys = ['APCA-API-KEY-ID', 'APCA-API-SECRET-KEY']
            if not all(k in credentials for k in required_keys):
                raise ValueError(f"Missing required credentials for 'api-key' mode. Required keys: {required_keys}")

            self.credentials = credentials

    def get_headers(self) -> dict:
        if self.auth_mode == 'client':
            raise NotImplementedError("Client-based authentication is not implemented yet.")

        elif self.auth_mode == 'api-key':
            return self.credentials

class MarketData(Authenticator):
    """
    Class to retrieve market data values. For more information, please refer to
    https://docs.alpaca.markets/us/reference/stockauctions-1

    Parameters
    ----------
    * credentials: dict
        A dictionary of credentials required for the chosen authentication mode.
            - 'APCA-API-KEY-ID': Your Alpaca API key ID
            - 'APCA-API-SECRET-KEY': Your Alpaca API secret key

    Examples
    --------
    >>> # to get bars
    >>> credentias = {'APCA-API-KEY-ID': 'apikeyid', 'APCA-API-SECRET-KEY': 'apikeysecret'}
    >>> market = MarketData(credentials=credentias)
    >>> market.get_bars(
    >>>     symbol='AAPL',
    >>>     timeframe='1H',
    >>>     start='2026-01-01',
    >>>     end='2026-01-01',
    >>>     adjustment='raw',
    >>>     limit='1000'
    >>> )
    """

    _BASE_URL = os.getenv("ALPACA-URL", "https://data.alpaca.markets/v2")  # market data endpoints base
    _CORPORATE_ACTIONS_URL = os.getenv(
        "ALPACA-CORPORATE-ACTIONS-URL", "https://data.alpaca.markets/v1/corporate-actions"
    )
    _TRADING_URL = os.getenv("ALPACA-TRADING-URL", "https://api.alpaca.markets/v2")  # trading endpoints base

    # Basic plan allows 200 requests/min. Spacing requests avoids 429s during backfills.
    _MAX_REQUESTS_PER_MINUTE = 200
    _MAX_RETRIES = 5

    _BAR_COLUMN_MAP = {
        "t": "timestamp_at",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "n": "trade_count",
        "vw": "vwap",
    }

    _BAR_COLUMNS = ["symbol"] + list(_BAR_COLUMN_MAP.values())

    _CALENDAR_COLUMNS = ["session_date", "open_et", "close_et", "settlement_date"]

    _CORPORATE_ACTION_COLUMNS = ["symbol", "ex_date", "type", "ratio", "cash_amount"]

    # Maps each collection returned by /v1/corporate-actions to a normalised `type`.
    _CORPORATE_ACTION_TYPE_MAP = {
        "forward_splits": "split",
        "reverse_splits": "split",
        "unit_splits": "split",
        "cash_dividends": "cash_dividend",
        "stock_dividends": "stock_dividend",
        "mergers": "merger",
        "spin_offs": "spin_off",
    }

    def __init__(self, credentials: dict):
        super().__init__(credentials=credentials, auth_mode='api-key')
        self._headers = self.get_headers()
        self._last_request_at = 0.0

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: str=None,
        end: str=None,
        adjustment: str = "raw",
        currency: str='USD',
        feed: str=None,
        sort: str='asc',
        limit=10000
    ) -> pd.DataFrame:
        """
        Function to retrieve stock bars from a given symbol in the values market.

        Parameters
        ----------
        * symbol: str,
            A comma-separated list of stock symbols.
            For example: 'APPL,TSLA'
        * timeframe: str,
             The timeframe represented by each bar in aggregation. You can use any of the following values.
                * [1-59]Min or [1-59]T, e.g. 5Min or 5T creates 5-minute aggregations
                * [1-23]Hour or [1-23]H, e.g. 12Hour or 12H creates 12-hour aggregations
                * 1Day or 1D creates 1-day aggregations
                * 1Week or 1W creates 1-week aggregations
                * [1,2,3,4,6,12]Month or [1,2,3,4,6,12]M, e.g. 3Month or 3M creates 3-month aggregations
        * start: str,
            The inclusive start of the interval.
            Format: RFC-3339 or YYYY-MM-DD.
        * end: str,
            The inclusive end of the interval.
            Format: RFC-3339 or YYYY-MM-DD.
        * adjustment: str='raw',
            Specifies the adjustments for the bars.
                * raw: no adjustments
                * split: adjust price and volume for forward and reverse stock splits
                * dividend: adjust price for cash dividends
                * spin-off: adjust price for spin-offs
                * all: apply all above adjustments
            You can combine multiple adjustments by separating them with a comma, e.g. split,spin-off.
            Bronze ingestion must always use 'raw': Alpaca applies adjustments with the factors known
            *today*, so an adjusted re-extraction is not reproducible.
        * currency: str='USD'
            The currency of all prices in ISO 4217 format. Default: USD.
        * feed: str=None,
            The source feed of the data. When omitted, Alpaca picks the best feed the
            subscription allows. Bronze ingestion always sets it explicitly.
                * sip: consolidated tape, 100% of the volume (>15 min old on the Basic plan)
                * iex: IEX only, ~2-3% of the volume
                * otc: over the counter
        * sort: str='asc',
            Sort direction of the returned bars, 'asc' or 'desc'.
        * limit: int=10000,
            The maximum number of data points to return in the response page.
            The API may return less, even if there are more available data points
            in the requested interval. Always check the next_page_token for more
            pages. The limit applies to the total number of data points, not per symbol!

        Returns
        -------
        pandas.DataFrame with columns
        ['symbol', 'timestamp_at', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'vwap'].
        `timestamp_at` is timezone-aware UTC and marks the **start** of the bar.
        """

        url = f"{self._BASE_URL}/stocks/bars"
        params = {
            "symbols": symbol,
            "timeframe": timeframe,
            "adjustment": adjustment,
            "currency": currency,
            "sort": sort,
            "limit": limit,
        }

        if feed: params["feed"] = feed
        if start: params["start"] = start
        if end: params["end"] = end

        raw_bars = {}

        for payload in self._paginate(url, params):
            for bar_symbol, bars in (payload.get("bars") or {}).items():
                raw_bars.setdefault(bar_symbol, []).extend(bars)

        return self._parse_bars(raw_bars)

    def get_calendar(self, start: str=None, end: str=None) -> pd.DataFrame:
        """
        Function to retrieve the market calendar (Trading API `GET /v2/calendar`).

        Without it there is no way to tell a minute with no trades apart from a closed
        market, and that distinction drives how gaps are filled downstream.

        Parameters
        ----------
        * start: str,
            The inclusive start of the interval. Format: YYYY-MM-DD.
        * end: str,
            The inclusive end of the interval. Format: YYYY-MM-DD.

        Returns
        -------
        pandas.DataFrame with columns ['session_date', 'open_et', 'close_et', 'settlement_date'].
        Times are strings in `America/New_York` ('HH:MM'), as returned by the API.
        """

        url = f"{self._TRADING_URL}/calendar"
        params = {}

        if start: params["start"] = start
        if end: params["end"] = end

        response = self._request(url, params)

        return self._parse_calendar(response.json())

    def get_corporate_actions(
        self,
        symbol: str,
        start: str=None,
        end: str=None,
        types: str=None,
        sort: str='asc',
        limit: int=1000,
    ) -> pd.DataFrame:
        """
        Function to retrieve corporate actions (`GET /v1/corporate-actions`).

        Splits and dividends are the discontinuities that make raw prices jump; the silver
        layer needs them to rebuild adjustment factors that are reproducible over time.

        Parameters
        ----------
        * symbol: str,
            A comma-separated list of stock symbols. For example: 'AAPL,TSLA'
        * start: str,
            The inclusive start of the interval, matched against `ex_date`. Format: YYYY-MM-DD.
        * end: str,
            The inclusive end of the interval, matched against `ex_date`. Format: YYYY-MM-DD.
        * types: str,
            Comma-separated list of corporate action types to return, e.g.
            'forward_split,reverse_split,cash_dividend'. Defaults to all types.
        * sort: str='asc',
            Sort direction of the returned actions, 'asc' or 'desc'.
        * limit: int=1000,
            Maximum number of data points per response page.

        Returns
        -------
        pandas.DataFrame with columns ['symbol', 'ex_date', 'type', 'ratio', 'cash_amount'].
        `type` is one of 'split', 'cash_dividend', 'stock_dividend', 'merger', 'spin_off'.
        """

        params = {"symbols": symbol, "sort": sort, "limit": limit}

        if start: params["start"] = start
        if end: params["end"] = end
        if types: params["types"] = types

        raw_actions = {}

        for payload in self._paginate(self._CORPORATE_ACTIONS_URL, params):
            for action_type, actions in (payload.get("corporate_actions") or {}).items():
                raw_actions.setdefault(action_type, []).extend(actions)

        return self._parse_corporate_actions(raw_actions)

    def _paginate(self, url: str, params: dict):
        """Yields every page of a paginated Alpaca response, following `next_page_token`."""

        params = dict(params)

        while True:
            payload = self._request(url, params).json()
            yield payload

            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
            params["page_token"] = next_page_token

    def _request(self, url: str, params: dict) -> requests.Response:
        """Performs a throttled GET, retrying on rate limiting and transient server errors."""

        for attempt in range(self._MAX_RETRIES):
            self._throttle()
            response = requests.get(url, headers=self._headers, params=params)

            is_retryable = response.status_code == 429 or response.status_code >= 500
            if not is_retryable or attempt == self._MAX_RETRIES - 1:
                response.raise_for_status()
                return response

            time.sleep(2 ** attempt)

    def _throttle(self) -> None:
        """Spaces consecutive requests to stay within the plan's requests/min budget."""

        min_interval = 60 / self._MAX_REQUESTS_PER_MINUTE
        elapsed = time.monotonic() - self._last_request_at

        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        self._last_request_at = time.monotonic()

    def _parse_bars(self, raw_bars: dict) -> pd.DataFrame:
        """Flattens the `{symbol: [bar, ...]}` payload into a tidy, multi-symbol frame."""

        if not raw_bars:
            return pd.DataFrame(columns=self._BAR_COLUMNS)

        frames = []
        for symbol, bars in raw_bars.items():
            if not bars:
                continue
            frame = pd.DataFrame(bars).rename(columns=self._BAR_COLUMN_MAP)
            frame.insert(0, "symbol", symbol)
            frames.append(frame)

        if not frames:
            return pd.DataFrame(columns=self._BAR_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        df["timestamp_at"] = pd.to_datetime(df["timestamp_at"], utc=True, format="ISO8601")

        return df[self._BAR_COLUMNS].sort_values(["symbol", "timestamp_at"], ignore_index=True)

    def _parse_calendar(self, raw_calendar: list) -> pd.DataFrame:
        if not raw_calendar:
            return pd.DataFrame(columns=self._CALENDAR_COLUMNS)

        df = pd.DataFrame(raw_calendar).rename(columns={"date": "session_date"})

        for column in self._CALENDAR_COLUMNS:
            if column not in df.columns:
                df[column] = None

        return df[self._CALENDAR_COLUMNS].sort_values("session_date", ignore_index=True)

    def _parse_corporate_actions(self, raw_actions: dict) -> pd.DataFrame:
        """Normalises the per-type collections into one row per (symbol, ex_date, type)."""

        rows = []

        for collection, actions in raw_actions.items():
            action_type = self._CORPORATE_ACTION_TYPE_MAP.get(collection, collection)

            for action in actions:
                rows.append({
                    "symbol": action.get("symbol"),
                    "ex_date": action.get("ex_date") or action.get("process_date"),
                    "type": action_type,
                    "ratio": self._extract_ratio(collection, action),
                    "cash_amount": action.get("rate") if action_type == "cash_dividend" else action.get("cash_rate"),
                })

        if not rows:
            return pd.DataFrame(columns=self._CORPORATE_ACTION_COLUMNS)

        df = pd.DataFrame(rows)[self._CORPORATE_ACTION_COLUMNS]
        df["ratio"] = pd.to_numeric(df["ratio"], errors="coerce")
        df["cash_amount"] = pd.to_numeric(df["cash_amount"], errors="coerce")

        return df.sort_values(["symbol", "ex_date", "type"], ignore_index=True)

    @staticmethod
    def _extract_ratio(collection: str, action: dict) -> float:
        """Returns the share multiplier of an action: 4.0 for a 4:1 split, 1.05 for a 5% stock dividend."""

        if collection in ("forward_splits", "reverse_splits", "unit_splits"):
            old_rate = action.get("old_rate")
            new_rate = action.get("new_rate")
            if old_rate in (None, 0) or new_rate is None:
                return None
            return float(new_rate) / float(old_rate)

        if collection == "stock_dividends":
            rate = action.get("rate")
            return None if rate is None else 1 + float(rate)

        return None

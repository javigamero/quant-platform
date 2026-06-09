import os

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
    
    _BASE_URL = os.getenv("ALPACA-URL")  # market data endpoints base

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

    def __init__(self, credentials: dict):
        super().__init__(credentials=credentials, auth_mode='api-key')
        self._headers = self.get_headers()

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: str=None,
        end: str=None,
        adjustment: str = "raw",
        currency: str='USD',
        limit=1000
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
        * currency: str='USD'
            The currency of all prices in ISO 4217 format. Default: USD.
        * limit: int=1000, 
            The maximum number of data points to return in the response page. 
            The API may return less, even if there are more available data points 
            in the requested interval. Always check the next_page_token for more 
            pages. The limit applies to the total number of data points, not per symbol!
        """
        
        url = f"{self._BASE_URL}/stocks/{symbol}/bars"
        params = {"timeframe": timeframe, 'adjustment': adjustment, 'currency': currency, "limit": limit}
        
        
        if start: params["start"] = start
        if end: params["end"] = end

        raw_bars = []
        
        while True:
            response = requests.get(url, headers=self._headers, params=params)
            response.raise_for_status()
            payload = response.json()
            raw_bars.extend(payload.get("bars", []))

            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break
            params["page_token"] = next_page_token

        return self._parse_bars(raw_bars)

    def _parse_bars(self, raw_bars: list) -> pd.DataFrame:
        if not raw_bars:
            return pd.DataFrame(columns=list(self._BAR_COLUMN_MAP.values()))
        df = pd.DataFrame(raw_bars).rename(columns=self._BAR_COLUMN_MAP)
        df["timestamp_at"] = pd.to_datetime(df["timestamp_at"], utc=True)
        return df[list(self._BAR_COLUMN_MAP.values())]

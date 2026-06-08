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
            - 'user': Your Alpaca API key ID
            - 'key': Your Alpaca API secret key
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
        start: str,
        end: str,
        timeframe: str = "1H",
        adjustment: str = "raw",
        limit=1000
    ) -> pd.DataFrame:
        
        url = f"{self._BASE_URL}/stocks/{symbol}/bars"
        params = {"timeframe": timeframe, "limit": limit}
        
        
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

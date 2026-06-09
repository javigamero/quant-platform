import sys
import os

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extractions.alpaca import MarketData

SYMBOL = "AAPL"
START = "2025-01-01"
END = "2025-01-02"
TIMEFRAME = "1H"

credentials = {'APCA-API-KEY-ID': os.getenv('APCA-API-KEY-ID'), 'APCA-API-SECRET-KEY': os.getenv('APCA-API-SECRET-KEY')}

if __name__ == "__main__":
    
    market = MarketData(credentials=credentials)

    print(f"Fetching {TIMEFRAME} bars for {SYMBOL} from {START} to {END}...")
    df = market.get_bars(symbol=SYMBOL, timeframe=TIMEFRAME, start=START, end=END)

    if df.empty:
        print("No data returned — check your API credentials and date range.")
        sys.exit(1)

    print(f"\nRetrieved {len(df)} bars:\n")
    print(df.to_string(index=False))

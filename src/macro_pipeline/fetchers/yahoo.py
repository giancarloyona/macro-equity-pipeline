import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import Optional
from macro_pipeline.fetchers.base import BaseFetcher


class YahooFetcher(BaseFetcher):
    def fetch_data(
        self, ticker: str, start_date: datetime, end_date: Optional[datetime] = None
    ) -> pd.DataFrame:

        data = yf.download(ticker, start=start_date, end=end_date, progress=False)

        if data.empty:
            return pd.DataFrame(columns=["value"], index=pd.Index([], name="date"))

        df = data[["Close"]].copy()
        df.columns = ["value"]
        df.index.name = "date"

        return df

from fredapi import Fred
import pandas as pd
from datetime import datetime
from typing import Optional
from macro_pipeline.fetchers.base import BaseFetcher


class FredFetcher(BaseFetcher):
    """
    Fetcher implementation for Federal Reserve Economic Data (FRED).
    """

    def __init__(self, api_key: Optional[str]):
        if not api_key:
            raise ValueError(
                "FRED API Key is required. Get one at https://fred.stlouisfed.org"
            )
        self.client = Fred(api_key=api_key)

    def fetch_data(
        self, ticker: str, start_date: datetime, end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Fetch macro series from FRED and standardize to DataFrame.
        """
        data: pd.Series = self.client.get_series(
            ticker, observation_start=start_date, observation_end=end_date
        )

        df = data.to_frame(name="value")
        df.index.name = "date"
        return df

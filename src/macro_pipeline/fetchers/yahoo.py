import pandas as pd
from datetime import datetime
from typing import Union, Optional, List
import yfinance as yf
from macro_pipeline.fetchers.base import BaseFetcher


class YahooFetcher(BaseFetcher):
    def fetch_data(
        self,
        ticker: Union[str, List[str]],
        start_date: datetime,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Fetches historical closing price data for one or more tickers from Yahoo Finance.

        Args:
            ticker (Union[str, List[str]]): The ticker symbol or a list of ticker symbols to fetch data for.
            start_date (datetime): The starting date from which to fetch the data.
            end_date (Optional[datetime], optional): The ending date up to which to fetch the data. Defaults to None (fetches up to the latest available date).

        Returns:
            pd.DataFrame: A DataFrame with the fetched closing price data.
                - If a single ticker is provided, the DataFrame will have a column labeled 'value' with closing prices.
                - If multiple tickers are provided, the columns will be labeled as 'value_{TICKER}' for each ticker.
                - The index is named 'date' and consists of the date values.

        Raises:
            ValueError: If the 'Close' column is not present in the downloaded data.
        """
        tickers = sorted([ticker] if isinstance(ticker, str) else list[str](ticker))

        if not tickers:
            return pd.DataFrame(columns=["value"], index=pd.Index([], name="date"))

        data = yf.download(tickers, start=start_date, end=end_date, progress=False)

        if data.empty:
            return pd.DataFrame(columns=["value"], index=pd.Index([], name="date"))

        data.index = pd.to_datetime(data.index)

        if "Close" not in data.columns.get_level_values(0).unique():
            raise ValueError("Coluna 'Close' não encontrada nos dados baixados.")

        if isinstance(data.columns, pd.MultiIndex):
            close_data = data.xs("Close", axis=1, level=0).copy()
        else:
            close_data = data[["Close"]].copy()

        if len(tickers) == 1:
            close_data.columns = ["value"]
        else:
            close_data.columns = [f"value_{ticker}" for ticker in tickers]

        close_data.index.name = "date"

        return close_data

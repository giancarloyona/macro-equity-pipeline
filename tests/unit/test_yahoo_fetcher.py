from typing import Any
import pandas as pd
from unittest.mock import patch
from datetime import datetime
from macro_pipeline.fetchers.yahoo import YahooFetcher


@patch("macro_pipeline.fetchers.yahoo.yf.download")
def test_yahoo_fetcher_standardizes_dataframe(mock_download):
    mock_data = pd.DataFrame(
        {"Close": [100.0, 105.0]},
        index=pd.to_datetime(["2023-01-01", "2023-01-02"]),
    )
    mock_download.return_value = mock_data

    fetcher = YahooFetcher()
    df = fetcher.fetch_data(ticker="SPY", start_date=datetime(2023, 1, 1))

    assert "value" in df.columns
    assert df.index.name == "date"
    assert not {"Open", "High", "Low", "Close"}.issubset(df.columns)


@patch("macro_pipeline.fetchers.yahoo.yf.download")
def test_yahoo_fetcher_accepts_list_of_tickers(mock_download):
    mock_data = pd.DataFrame(
        {
            ("Close", "QQQ"): [100.0, 105.0],
            ("Close", "SPY"): [350.0, 352.0],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-02"]),
    )
    mock_download.return_value = mock_data

    fetcher = YahooFetcher()
    df = fetcher.fetch_data(ticker=["SPY", "QQQ"], start_date=datetime(2023, 1, 1))

    assert list[Any](df.columns) == ["value_QQQ", "value_SPY"]
    assert df.index.name == "date"
    assert df.loc["2023-01-01", "value_SPY"] == 350.0
    assert df.loc["2023-01-01", "value_QQQ"] == 100.0

import pytest
import pandas as pd
from unittest.mock import patch
from datetime import datetime
from macro_pipeline.fetchers.fred import FredFetcher


@pytest.fixture
def mock_fred_api():
    with patch("macro_pipeline.fetchers.fred.Fred") as mocked_fred:
        yield mocked_fred


def test_fred_fetcher_requires_api_key():
    with pytest.raises(ValueError) as excinfo:
        FredFetcher(api_key=None)
    assert "FRED API Key is required" in str(excinfo.value)


def test_fred_fetcher_returns_valid_dataframe(mock_fred_api):
    mock_instance = mock_fred_api.return_value
    mock_instance.get_series.return_value = pd.Series(
        [1.5, 1.6], index=pd.to_datetime(["2023-01-01", "2023-02-01"]), name="value"
    )

    fetcher = FredFetcher(api_key="fake_key")
    df = fetcher.fetch_data(ticker="CPIAUCSL", start_date=datetime(2023, 1, 1))

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "value" in df.columns
    assert df.index.name == "date"

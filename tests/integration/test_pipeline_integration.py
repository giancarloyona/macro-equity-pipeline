from datetime import datetime
from unittest.mock import patch
import pandas as pd
from macro_pipeline.main import run_pipeline

@patch("macro_pipeline.fetchers.fred.Fred")
@patch("macro_pipeline.fetchers.yahoo.yf.download")
def test_full_pipeline_flow(mock_yf, mock_fred_api):
    mock_fred_api.return_value.get_series.return_value = pd.Series([100, 101], index=pd.to_datetime(["2023-01-01", "2023-02-01"]))
    mock_yf.return_value = pd.DataFrame({"Close": [100, 105]}, index=pd.to_datetime(["2023-01-01", "2023-01-02"]))
    result = run_pipeline("TEST_TICKER", "TEST_MACRO", datetime(2023, 1, 1))
    
    assert not result.empty
    assert "real_return" in result.columns
    assert "cumulative_real_return" in result.columns
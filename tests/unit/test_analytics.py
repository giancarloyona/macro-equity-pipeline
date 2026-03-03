import pytest
import pandas as pd
from macro_pipeline.processors.analytics import AnalyticsProcessor

def test_merge_macro_and_equity_data():
    equity_df = pd.DataFrame(
        {"value": [100.0, 101.0, 102.0]},
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"])
    )
    equity_df.index.name = "date"

    macro_df = pd.DataFrame(
        {"value": [250.0]}, 
        index=pd.to_datetime(["2023-01-01"])
    )
    macro_df.index.name = "date"

    processor = AnalyticsProcessor()
    merged_df = processor.merge_datasets(equity_df, macro_df, label_macro="cpi")

    assert len(merged_df) == 3
    assert "value_equity" in merged_df.columns
    assert "value_cpi" in merged_df.columns
    assert merged_df["value_cpi"].iloc[2] == 250.0

def test_calculate_real_return_logic():
    data = {
        "value_equity": [100.0, 110.0],
        "value_cpi": [100.0, 110.0]
    }
    df = pd.DataFrame(data, index=pd.to_datetime(["2023-01-01", "2023-02-01"]))
    
    processor = AnalyticsProcessor()
    result_df = processor.calculate_real_return(df, equity_col="value_equity", macro_col="value_cpi")
    
    assert result_df["real_return"].iloc[1] == pytest.approx(0.0)
    assert pd.isna(result_df["real_return"].iloc[0])
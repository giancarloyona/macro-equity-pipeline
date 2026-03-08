import pandas as pd
from macro_pipeline.processors.analytics import AnalyticsProcessor


def test_merge_macro_and_equity_data():
    equity_df = pd.DataFrame(
        {"value": [100.0, 101.0, 102.0]},
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )
    equity_df.index.name = "date"

    macro_df = pd.DataFrame(
        {"value": [250.0]},
        index=pd.to_datetime(["2023-01-01"]),
    )
    macro_df.index.name = "date"

    processor = AnalyticsProcessor()
    merged_df = processor.merge_datasets(equity_df, macro_df, label_macro="cpi")

    assert len(merged_df) == 3
    assert "value" in merged_df.columns
    assert "value_cpi" in merged_df.columns
    assert merged_df["value_cpi"].iloc[2] == 250.0


def test_merge_multiple_equity_and_macro_data():
    equity_df = pd.DataFrame(
        {
            "value_SPY": [100.0, 101.0, 102.0],
            "value_QQQ": [300.0, 305.0, 310.0],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    )

    macro_df = pd.DataFrame(
        {
            "value": [250.0, 255.0],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-03"]),
    )

    processor = AnalyticsProcessor()
    merged_df = processor.merge_datasets(equity_df, macro_df, label_macro="cpi")

    assert "value_SPY" in merged_df.columns
    assert "value_QQQ" in merged_df.columns
    assert "value_cpi" in merged_df.columns
    assert merged_df["value_cpi"].iloc[0] == 250.0
    assert merged_df["value_cpi"].iloc[2] == 255.0

import pandas as pd
from macro_pipeline.processors.analytics import AnalyticsProcessor
import pytest
import numpy as np


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


def test_get_performance_metrics_calculation():
    data = {
        "real_return_AAPL": [np.nan, 0.01, -0.02, 0.03],
        "cumulative_real_return_AAPL": [0.0, 0.01, -0.0102, 0.019494],
    }
    df = pd.DataFrame(
        data,
        index=pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"]),
    )

    processor = AnalyticsProcessor()
    metrics = processor.get_performance_metrics(df, equity_cols="AAPL")

    assert isinstance(metrics, dict)
    assert metrics["ticker"] == "AAPL"

    expected_vol = np.std([0.01, -0.02, 0.03], ddof=1) * np.sqrt(252)
    assert metrics["volatility"] == pytest.approx(expected_vol)

    assert metrics["max_drawdown"] == pytest.approx(-0.02)

    assert metrics["total_real_return"] == pytest.approx(0.019494)


def test_get_performance_metrics_multi_ticker():
    data = {
        "real_return_AAPL": [0.01, 0.02],
        "cumulative_real_return_AAPL": [0.01, 0.0302],
        "real_return_TSLA": [0.05, -0.01],
        "cumulative_real_return_TSLA": [0.05, 0.0395],
    }
    df = pd.DataFrame(data, index=pd.to_datetime(["2023-01-01", "2023-01-02"]))

    processor = AnalyticsProcessor()
    metrics_list = processor.get_performance_metrics(df, equity_cols=["AAPL", "TSLA"])

    assert isinstance(metrics_list, list)
    assert len(metrics_list) == 2
    assert metrics_list[0]["ticker"] == "AAPL"
    assert metrics_list[1]["ticker"] == "TSLA"

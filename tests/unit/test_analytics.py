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


def test_get_correlation_matrix_logic():
    data = {
        "real_return_AAPL": [0.01, 0.02, 0.03],
        "real_return_MSFT": [0.01, 0.02, 0.03],
        "real_return_GOLD": [-0.01, -0.02, -0.03],
    }
    df = pd.DataFrame(data)

    processor = AnalyticsProcessor()
    tickers = ["AAPL", "MSFT", "GOLD"]
    corr_matrix = processor.get_correlation_matrix(df, tickers)

    assert list(corr_matrix.columns) == tickers
    assert list(corr_matrix.index) == tickers

    assert corr_matrix.loc["AAPL", "AAPL"] == pytest.approx(1.0)
    assert corr_matrix.loc["AAPL", "MSFT"] == pytest.approx(1.0)
    assert corr_matrix.loc["AAPL", "GOLD"] == pytest.approx(-1.0)


def test_correlation_with_nan_values():
    data = {
        "real_return_A": [np.nan, 0.01, 0.05, 0.02],
        "real_return_B": [np.nan, 0.02, 0.10, 0.04],
    }
    df = pd.DataFrame(data)

    processor = AnalyticsProcessor()
    corr_matrix = processor.get_correlation_matrix(df, ["A", "B"])

    assert corr_matrix.loc["A", "B"] == pytest.approx(1.0)

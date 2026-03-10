import pandas as pd
import numpy as np
from typing import List, Union, Dict


class AnalyticsProcessor:
    """
    Handles data transformation, merging, and financial calculations.
    """

    def merge_datasets(
        self,
        equity_df: pd.DataFrame,
        macro_df: pd.DataFrame,
        label_macro: str = "macro",
    ) -> pd.DataFrame:
        """
        Merges daily equity data with monthly/quarterly macro data.
        Uses Forward Fill to align time series.
        """

        macro = macro_df.rename(columns={"value": f"value_{label_macro}"})
        merged = pd.merge(
            equity_df, macro, left_index=True, right_index=True, how="left"
        )
        merged[f"value_{label_macro}"] = merged[f"value_{label_macro}"].ffill()
        return merged

    def calculate_real_return(
        self,
        df: pd.DataFrame,
        equity_cols: Union[str, List[str]],
        macro_col: str = "value_macro",
    ) -> pd.DataFrame:
        """
        Calculates the inflation-adjusted (real) return for one or multiple equity columns.
        Formula: ((1 + nominal_ret) / (1 + inflation_ret)) - 1
        """
        df = df.copy()

        if isinstance(equity_cols, str):
            equity_cols = [equity_cols]

        if macro_col not in df.columns:
            raise ValueError(f"Coluna macroeconômica '{macro_col}' não encontrada.")

        inflation_ret = df[macro_col].pct_change()

        for equity_col in equity_cols:
            if equity_col not in df.columns:
                raise ValueError(f"Coluna de ativo '{equity_col}' não encontrada.")

            nominal_ret = df[equity_col].pct_change()
            df[f"real_return_{equity_col}"] = (
                (1 + nominal_ret) / (1 + inflation_ret)
            ) - 1
            df[f"cumulative_real_return_{equity_col}"] = (
                1 + df[f"real_return_{equity_col}"].fillna(0)
            ).cumprod() - 1

        return df

    def get_performance_metrics(
        self, df: pd.DataFrame, equity_cols: List[str]
    ) -> Union[Dict, List[Dict]]:
        """
        Calculates risk and performance metrics for one or more assets
        based on the already calculated real return columns.
        """
        if len(equity_cols) == 1:
            equity_cols = ["value"]

        metrics_results = []

        for col in equity_cols:
            real_ret_col = f"real_return_{col}"
            cum_ret_col = f"cumulative_real_return_{col}"

            if real_ret_col not in df.columns:
                continue

            returns = df[real_ret_col].dropna()
            if returns.empty:
                continue

            vol_anual = returns.std() * np.sqrt(252)

            ret_medio_anual = returns.mean() * 252

            sharpe = ret_medio_anual / vol_anual if vol_anual != 0 else 0

            wealth_index = 1 + df[cum_ret_col]
            previous_peaks = wealth_index.cummax()
            drawdowns = (wealth_index - previous_peaks) / previous_peaks
            max_drawdown = drawdowns.min()

            metrics_results.append(
                {
                    "ticker": col,
                    "volatility": vol_anual,
                    "sharpe_ratio": sharpe,
                    "max_drawdown": max_drawdown,
                    "total_real_return": df[cum_ret_col].iloc[-1],
                }
            )

        return metrics_results

    def get_correlation_matrix(
        self, df: pd.DataFrame, equity_cols: List[str]
    ) -> pd.DataFrame:
        """
        Calculates the correlation matrix between the real returns of the selected assets.
        """
        cols_to_corr = [f"real_return_{ticker}" for ticker in equity_cols]

        corr_matrix = df[cols_to_corr].corr()

        corr_matrix.columns = [
            c.replace("real_return_", "") for c in corr_matrix.columns
        ]
        corr_matrix.index = [c.replace("real_return_", "") for c in corr_matrix.index]

        return corr_matrix

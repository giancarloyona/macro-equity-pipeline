import pandas as pd
from typing import List, Union


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

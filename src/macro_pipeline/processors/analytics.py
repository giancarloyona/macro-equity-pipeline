import pandas as pd


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

        equity = equity_df.rename(columns={"value": "value_equity"})
        macro = macro_df.rename(columns={"value": f"value_{label_macro}"})

        merged = pd.merge(equity, macro, left_index=True, right_index=True, how="left")

        merged[f"value_{label_macro}"] = merged[f"value_{label_macro}"].ffill()

        return merged

    def calculate_real_return(
        self,
        df: pd.DataFrame,
        equity_col: str = "value_equity",
        macro_col: str = "value_macro",
    ) -> pd.DataFrame:
        """
        Calculates the inflation-adjusted (real) return using the Fisher equation.
        Formula: ((1 + nominal_ret) / (1 + inflation_ret)) - 1
        """
        df = df.copy()

        nominal_ret = df[equity_col].pct_change()
        inflation_ret = df[macro_col].pct_change()

        df["real_return"] = ((1 + nominal_ret) / (1 + inflation_ret)) - 1
        df["cumulative_real_return"] = (1 + df["real_return"].fillna(0)).cumprod() - 1

        return df

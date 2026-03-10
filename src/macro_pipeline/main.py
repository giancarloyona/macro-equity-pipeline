import os
from dotenv import load_dotenv
from datetime import datetime
from macro_pipeline.fetchers.fred import FredFetcher
from macro_pipeline.fetchers.yahoo import YahooFetcher
from macro_pipeline.processors.analytics import AnalyticsProcessor


def run_pipeline(equity_tickers: list[str], macro_series: str, start_date: datetime):
    """
    Runs the macro-equity real return analysis pipeline.

    Args:
        equity_tickers (list[str]): List of equity ticker symbols (e.g., ["SPY", "QQQ"]).
        macro_series (str): FRED macroeconomic series name (e.g., "CPIAUCSL").
        start_date (datetime): Start date for the analysis period.

    Returns:
        pd.DataFrame: DataFrame with merged equity and macro data, and columns for real and cumulative real return(s).

    Steps:
        1. Loads FRED credentials from environment (.env file).
        2. Fetches historical equity prices and macroeconomic series.
        3. Merges and aligns series by date (forward-filling macro data).
        4. Calculates real and cumulative real returns for each asset.

    Example:
        >>> run_pipeline(["SPY", "QQQ"], "CPIAUCSL", datetime(2010, 1, 1))
    """

    load_dotenv()
    fred_key = os.getenv("FRED_API_KEY")

    fred_fetcher = FredFetcher(api_key=fred_key)
    yf_fetcher = YahooFetcher()
    processor = AnalyticsProcessor()

    if len(equity_tickers) == 1:
        equity_cols = ["value"]
    else:
        equity_cols = [f"value_{ticker}" for ticker in equity_tickers]

    print(f"Starting pipeline for {equity_tickers} vs {macro_series}...")

    print("Fetching data from FRED and Yahoo Finance...")
    df_equity = yf_fetcher.fetch_data(equity_tickers, start_date=start_date)
    df_macro = fred_fetcher.fetch_data(macro_series, start_date=start_date)

    print("Processing and aligning datasets...")
    merged_df = processor.merge_datasets(
        df_equity, df_macro, label_macro=macro_series.lower()
    )

    final_df = processor.calculate_real_return(
        merged_df, equity_cols=equity_cols, macro_col=f"value_{macro_series.lower()}"
    )

    print("\nPipeline Finished Successfully!")
    print(f"Total data points: {len(final_df)}")
    print("Latest Real Returns (Cumulative):")

    if len(equity_cols) > 1:
        adj_cols = [
            x.replace("_value_", "_") if "real" in x else x for x in final_df.columns
        ]
        final_df.columns = adj_cols

    return final_df


if __name__ == "__main__":
    start = datetime(2010, 1, 1)
    run_pipeline(
        equity_tickers=["SPY", "EWZ"], macro_series="CPIAUCSL", start_date=start
    )

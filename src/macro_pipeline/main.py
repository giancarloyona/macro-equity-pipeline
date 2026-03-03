import os
from datetime import datetime
from dotenv import load_dotenv

from macro_pipeline.fetchers.fred import FredFetcher
from macro_pipeline.fetchers.yahoo import YahooFetcher
from macro_pipeline.processors.analytics import AnalyticsProcessor


def run_pipeline(equity_ticker: str, macro_series: str, start_date: datetime):

    load_dotenv()
    fred_key = os.getenv("FRED_API_KEY")
    fred_fetcher = FredFetcher(api_key=fred_key)
    yf_fetcher = YahooFetcher()
    processor = AnalyticsProcessor()

    print(f"Starting pipeline for {equity_ticker} vs {macro_series}...")

    print("Fetching data from FRED and Yahoo Finance...")
    df_equity = yf_fetcher.fetch_data(equity_ticker, start_date=start_date)
    df_macro = fred_fetcher.fetch_data(macro_series, start_date=start_date)

    print("Processing and aligning datasets...")
    merged_df = processor.merge_datasets(
        df_equity, df_macro, label_macro=macro_series.lower()
    )

    final_df = processor.calculate_real_return(
        merged_df, equity_col="value_equity", macro_col=f"value_{macro_series.lower()}"
    )

    print("\nPipeline Finished Successfully!")
    print(f"Total data points: {len(final_df)}")
    print(
        f"Latest Real Return (Cumulative): {final_df['cumulative_real_return'].iloc[-1]:.2%}"
    )

    return final_df


if __name__ == "__main__":
    start = datetime(2010, 1, 1)
    run_pipeline(equity_ticker="SPY", macro_series="CPIAUCSL", start_date=start)

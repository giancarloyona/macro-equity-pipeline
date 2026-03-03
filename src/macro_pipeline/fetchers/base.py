from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
from typing import Optional


class BaseFetcher(ABC):
    """
    Abstract Base Class for all data fetchers in the macro-equity-pipeline.
    Ensures a consistent interface for different data providers (FRED, Yahoo, etc).
    """

    @abstractmethod
    def fetch_data(
        self, ticker: str, start_date: datetime, end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Fetches data from the provider and returns a standardized pandas DataFrame.

        Args:
            ticker: The symbol or ID for the data series.
            start_date: Start of the period.
            end_date: End of the period (defaults to now).

        Returns:
            pd.DataFrame: Data with at least 'date' and 'value' columns.
        """
        pass

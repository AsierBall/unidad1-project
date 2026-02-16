from pathlib import Path
from typing import Protocol
import pandas as pd

from ..logging import get_logger

logger = get_logger(__name__)


class Writer(Protocol):
    """Writer class that writes a pandas DataFrame
    to a destination file.
    """
    def write(self, data_frame: pd.DataFrame, file_path: Path) -> None:
        """
        Write a DataFrame to the configured destination.
        """
        ...



class WriterCsv(Writer):
    """
    CSV implementation of the Writer interface.

    - Creates the file if it does not exist.
    - Appends data if it exists.
    - Validates column consistency before appending.
    """

    def _create_directory_if_not_exist(self, file_path: Path) -> None:
        """
        Ensure that the parent directory exists.
        """
        file_path.parent.mkdir(parents=True, exist_ok=True)



    def _validate_columns(self, df: pd.DataFrame, file_path: Path) -> None:
        """
        Validate that the DataFrame columns match the existing CSV file.

        :param df: DataFrame to validate.
        :type df: pandas.DataFrame
        :raises ValueError: If column names do not match the existing file.
        """
        existing_df = pd.read_csv(file_path, nrows=0)
        if list(existing_df.columns) != list(df.columns):
            raise ValueError(f"Column mismatch.\n"
                    f"Existing: {list(existing_df.columns)}\n"
                    f"New: {list(df.columns)}")


    def write(self, data_frame: pd.DataFrame, file_path: Path) -> None:
        """
        Append the provided DataFrame to the CSV file.

        If the file does not exist, it is created.
        If it exists, column consistency is validated before appending.

        :param data_frame: DataFrame to be written.
        :type data_frame: pandas.DataFrame
        """
        self._create_directory_if_not_exist(file_path)

        file_exists = file_path.exists()

        if file_exists:
            self._validate_columns(data_frame, file_path)

        data_frame.to_csv(
            file_path,
            mode="a",
            index=False,
            header=not file_exists
        )

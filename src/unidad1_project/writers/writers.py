from pathlib import Path
from typing import Protocol
import pandas as pd

# TODO: writer a formato comprimido `parquet`
# TODO: writer a formato comprimido `excel` ????
# TODO: writer a `json`

class Writer(Protocol):
    """Interface for objects capable of writing a pandas DataFrame
    to a destination file.
    """
    def write(self, data_frame: pd.DataFrame) -> None:
        """
        Write a DataFrame to the configured destination.

        :param data_frame: DataFrame to be written.
        :type data_frame: pandas.DataFrame
        """
        ...

class WriterCsv(Writer):
    """
        CSV implementation of the Writer interface.

        - Creates the file if it does not exist.
        - Appends data if it exists.
        - Validates column consistency before appending.
        """
    def __init__(self, file_path: Path)-> None:
        """
        Initialize the CSV writer.

        """
        self._file_path = file_path


    def _create_directory_if_not_exist(self) -> None:
        """
        Ensure that the parent directory exists.
        """
        self._file_path.parent.mkdir(parents=True, exist_ok=True)


    def _validate_columns(self, df: pd.DataFrame) -> None:
        """
        Validate that the DataFrame columns match the existing CSV file.

        :param df: DataFrame to validate.
        :type df: pandas.DataFrame
        :raises ValueError: If column names do not match the existing file.
        """
        existing_df = pd.read_csv(self._file_path, nrows=0)

        if list(existing_df.columns) != list(df.columns):
            raise ValueError(f"Column mismatch.\n"
                    f"Existing: {list(existing_df.columns)}\n"
                    f"New: {list(df.columns)}")

    def write(self, data_frame: pd.DataFrame) -> None:
        """
        Append the provided DataFrame to the CSV file.

        If the file does not exist, it is created.
        If it exists, column consistency is validated before appending.

        :param data_frame: DataFrame to be written.
        :type data_frame: pandas.DataFrame
        """
        self._create_directory_if_not_exist()

        file_exists = self._file_path.exists()
        if file_exists:
            self._validate_columns(data_frame)

        data_frame.to_csv(
            self._file_path,
            mode="a",
            index=False,
            header=not file_exists
        )

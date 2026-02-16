import json
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
    CSV implementation of the Writer.

    - Creates the file if it does not exist.
    - Appends data if it exists.
    - Validates column consistency before appending.
    """

    def _create_directory_if_not_exist(self, file_path: Path) -> None:
        """
        Ensure that the parent directory exists.
        """
        parent_dir = file_path.parent

        if not parent_dir.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Directory created: {parent_dir}")
        else:
            logger.debug(f"Directory already exists: {parent_dir}")



    def _validate_columns(self, df: pd.DataFrame, file_path: Path) -> None:
        """
        Validate that the DataFrame columns match the existing CSV file.

        :param df: DataFrame to validate.
        :type df: pandas.DataFrame
        :raises ValueError: If column names do not match the existing file.
        """
        existing_df = pd.read_csv(file_path, nrows=0)
        if list(existing_df.columns) != list(df.columns):
            logger.error(
                f"Column mismatch. Existing: {list(existing_df.columns)}, New: {list(df.columns)}"
            )
            raise ValueError(f"Column mismatch.\n"
                    f"Existing: {list(existing_df.columns)}\n"
                    f"New: {list(df.columns)}")
        else:
            logger.debug(f"Columns validated successfully: {list(df.columns)}")


    def write(self, data_frame: pd.DataFrame, file_path: Path) -> None:
        """
        Append the provided DataFrame to the CSV file.

        If the file does not exist, it is created.
        If it exists, column consistency is validated before appending.

        :param data_frame: DataFrame to be written.
        :type data_frame: pandas.DataFrame
        :raises ValueError: If the existing file has incompatible columns.
        """
        self._create_directory_if_not_exist(file_path)

        file_exists = file_path.exists()

        if file_exists:
            self._validate_columns(data_frame, file_path)
            logger.info(f"Appending {len(data_frame)} rows to existing file: {file_path}")
        else:
            logger.info(f"Creating new file and writing {len(data_frame)} rows: {file_path}")

        data_frame.to_csv(
            file_path,
            mode="a",
            index=False,
            header=not file_exists
        )

        logger.debug(f"Write operation completed successfully: {file_path}")


class WriterJson(Writer):
    """
    JSON implementation of the Writer.

    - Creates the file if it does not exist.
    - Appends records in JSON Lines format.
    - Validates column consistency before appending.
    """

    def _create_directory_if_not_exist(self, file_path: Path) -> None:
        """
        Ensure that the parent directory exists.
        """
        parent_dir = file_path.parent

        if not parent_dir.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Directory created: {parent_dir}")
        else:
            logger.debug(f"Directory already exists: {parent_dir}")


    def _validate_columns(self, df: pd.DataFrame, file_path: Path) -> None:
        """
        Validate that the DataFrame columns match the existing JSON file.

        :param df: DataFrame to validate.
        :type df: pandas.DataFrame
        :raises ValueError: If column names do not match the existing file.
        """
        if not file_path.exists():
            return

        with file_path.open("r", encoding="utf-8") as f:
            first_line = f.readline()

            if not first_line.strip():
                return

            existing_record = json.loads(first_line)
            existing_columns = list(existing_record.keys())

        if list(df.columns) != existing_columns:
            logger.error(
                f"Column mismatch. Existing: {list(existing_columns)}, New: {list(df.columns)}"
            )
            raise ValueError(
                f"Column mismatch.\n"
                f"Existing: {existing_columns}\n"
                f"New: {list(df.columns)}"
            )
        else:
            logger.debug(f"Columns validated successfully: {list(df.columns)}")


    def write(self, data_frame: pd.DataFrame, file_path: Path) -> None:
        """
        Append the provided DataFrame to the JSON file in JSON Lines format.

        :param data_frame: DataFrame to be written.
        :type data_frame: pandas.DataFrame
        :raises ValueError: If the existing file has incompatible columns.
        """
        self._create_directory_if_not_exist(file_path)

        if file_path.exists():
            self._validate_columns(data_frame, file_path)
            logger.info(f"Appending {len(data_frame)} rows to existing file: {file_path}")
        else:
            logger.info(f"Creating new file and writing {len(data_frame)} rows: {file_path}")

        with file_path.open("a", encoding="utf-8") as f:
            for record in data_frame.to_dict(orient="records"):
                json.dump(record, f)
                f.write("\n")

        logger.debug(f"Write operation completed successfully: {file_path}")

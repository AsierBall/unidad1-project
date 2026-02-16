import csv
from pathlib import Path
from typing import Generator, Protocol
import pandas as pd
from ..logging import get_logger

logger = get_logger(__name__)

CSV_SNIFFER_ENCODING = "utf-8"
CSV_SNIFFER_SIZE = 4096

class Reader(Protocol):
    """A reader class that reads a file"""

    def read(self, file_path: Path) -> Generator:
        """function that reads a file as a generator

        :param file_path: path to the file
        :type file_path: Path
        :yield: generator for a file stream
        :rtype: Generator
        """
        ...


class CSVReaderPandas:
    """
    CSV reader implementation using pandas with chunk-based loading support.
    This class allows to read big CSV files by chunks using a generator

    :param chunk_size: Number of rows to be processed in each chunk.
    :type chunk_size: int
    :raises ValueError: Si el `chunk_size` no es un entero positivo.
    """

    def __init__(self, chunk_size: int):
        if chunk_size <= 0:
            raise ValueError("The chunk_size must be a positive integer.")
        self._chunk_size = chunk_size

    def _detect_delimiter(self, file_path: Path) -> str:
        """
        Automatically detects the CSV delimiter

        :param file_path: Path to the file that we want to sniff
        :type file_path: Path
        :return: The delimiter character (ej. ',', ';').
        :rtype: str
        :raises ValueError: If the file is empty or the function can't determine the delimitator.
        """
        try:
            with open(file_path, "r", newline="", encoding=CSV_SNIFFER_ENCODING) as f:
                sample = f.read(CSV_SNIFFER_SIZE)
                if not sample.strip():
                    raise ValueError(f"The file '{file_path}' is empty.")

                dialect = csv.Sniffer().sniff(sample)
                return dialect.delimiter
        except csv.Error:
            raise ValueError(f"Could not determine the delimiter for '{file_path}'. Is it a valid CSV?")

    def read(self, file_path: Path) -> Generator[pd.DataFrame, None, None]:
        """
        Iteratively read the CSV file and return a DataFrame generator.

        This method validates the file's existence, detects its format, and starts
        reading in segments according to the ``chunk_size`` defined in the constructor.

        :param file_path: Path to the CSV file to be read.
        :type file_path: Path
        :yield: A fragment of the file loaded into a ``pandas.DataFrame`` object.
        :rtype: Generator[pd.DataFrame, None, None]
        :raises FileNotFoundError: If the file does not exist at the provided path.
        :raises pd.errors.EmptyDataError: If the file contains no readable data.
        :raises pd.errors.ParserError: If a structural error occurs during parsing.
        :raises Exception: For any other unexpected errors during reading.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: '{file_path}'")

        if file_path.suffix.lower() != ".csv":
            logger.warning(f"The file extension for '{file_path.name}' is not .csv")

        try:
            delimiter = self._detect_delimiter(file_path)
        except Exception as e:
            logger.error(f"Format error: {e}")
            raise

        try:
            reader_obj = pd.read_csv(
                file_path,
                chunksize=self._chunk_size,
                sep=delimiter,
                on_bad_lines="warn",
            )

            for chunk in reader_obj:
                yield chunk

        except pd.errors.EmptyDataError:
            logger.error(f"The file '{file_path}' is empty or contains no readable data.")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Parsing error in '{file_path}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while reading '{file_path}': {e}")
            raise


class JSONReaderPandas:
    """
    JSON reader implementation using pandas.
    This class reads JSON files and yields them as DataFrames to maintain
    compatibility with the Reader protocol.
    """

    def read(self, file_path: Path) -> Generator:
        """
        Reads the JSON file and yields it as a DataFrame fragment.

        This method validates the file's existence and attempts to load the content.
        For standard JSON files, it yields the entire content as a single chunk.

        :param file_path: Path to the JSON file to be read.
        :type file_path: Path
        :yield: A DataFrame representing the JSON content.
        :rtype: Generator[pd.DataFrame, None, None]
        :raises FileNotFoundError: If the file does not exist.
        :raises ValueError: If the JSON is malformed.
        :raises Exception: For any other unexpected errors.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: '{file_path}'")

        if file_path.suffix.lower() != ".json":
            logger.warning(f"The file extension for '{file_path.name}' is not .json")

        try:
            df = pd.read_json(file_path)

            if df.empty:
                logger.warning(f"The file '{file_path}' returned an empty DataFrame.")

            yield df

        except ValueError as e:
            logger.error(f"JSON Decode error in '{file_path}': {e}")
            raise ValueError(f"The file '{file_path}' is not a valid JSON.") from e
        except Exception as e:
            logger.error(f"Unexpected error while reading '{file_path}': {e}")
            raise

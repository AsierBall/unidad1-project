from pathlib import Path
from typing import Protocol
import pandas as pd

# TODO: writer a formato comprimido `parquet`
# TODO: writer a formato comprimido `excel` ????
# TODO: writer a `json`

class Writer(Protocol):
    """

    """
    def write(self, data_frame: pd.DataFrame) -> None: ...

class WriterCsv(Writer):
    def __init__(self, file_path: Path):
        """
        CSV implementation of the Writer interface.

        - Creates the file if it does not exist.
        - Appends data if it exists.
        - Validates column consistency before appending.
        """

        self._file_path = file_path



    def _create_directory_if_not_exist(self) -> None:
        self._file_path.parent.mkdir(parents=True, exist_ok=True)


    def _validate_columns(self, df: pd.DataFrame) -> None:
        existing_df = pd.read_csv(self._file_path, nrows=0)

        if list(existing_df.columns) != list(df.columns):
            raise ValueError(f"Column mismatch.\n"
                    f"Existing: {list(existing_df.columns)}\n"
                    f"New: {list(df.columns)}")

    def write(self, data_frame: pd.DataFrame) -> None:
        print(f"writing file... {self._file_path}")
        print(data_frame)

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
        # TODO: logica de guardado
        # Abrir archivo en modo append y añadir el contenido del df

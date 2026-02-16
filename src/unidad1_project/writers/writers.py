from pathlib import Path
from typing import Protocol
import pandas as pd

# TODO: writer a formato comprimido `parquet`
# TODO: writer a formato comprimido `excel` ????
# TODO: writer a `json`

class Writer(Protocol):
    """

    """
    def write(self, data_frame: pd.DataFrame, file_path: Path) -> None: ...

class WriterCsv(Writer):
    def _create_directory_if_not_exist(self, file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)


    def _validate_columns(self, df: pd.DataFrame, file_path: Path) -> None:
        existing_df = pd.read_csv(file_path, nrows=0)

        if list(existing_df.columns) != list(df.columns):
            raise ValueError(f"Column mismatch.\n"
                    f"Existing: {list(existing_df.columns)}\n"
                    f"New: {list(df.columns)}")

    def write(self, data_frame: pd.DataFrame, file_path: Path) -> None:
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
        # TODO: logica de guardado
        # Abrir archivo en modo append y añadir el contenido del df

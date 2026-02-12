from pathlib import Path
from typing import Protocol
from pandas import DataFrame

class Writer(Protocol):
    def write(self, data_frame: DataFrame) -> None: ...

class WriterCsv(Writer):
    def __init__(self, file_path: Path):
        self._file_path = file_path

    def write(self, data_frame: DataFrame) -> None:
        print(f"writing file... {self._file_path}")

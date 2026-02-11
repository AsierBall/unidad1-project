from abc import ABC, abstractmethod
import csv
from pathlib import Path
from typing import Iterator

import pandas as pd


class Reader(ABC):

    

    @abstractmethod
    def read(self) -> Iterator:
        pass




class CSVReader():

    def __init__(self, file: Path):
        self._file_path = file

    def _detect_delimiter(self):
        with open(self._file_path, newline='', encoding='utf-8') as f:
            sample = f.read(100)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            delimiter = dialect.delimiter
            return delimiter


    def read(self):
        # if not self._file_path.is_file():
        #     raise TypeError("Debe ser un archivo")
        # if self._file_path.suffix.lower() == ".csv":
        #     raise TypeError("El archivo debe ser csv")
        # try:
        #     lector = pd.read_csv(self._file_path, chunksize=1)
        #     return lector
        # except Exception as e:
        #     pass
        delimiter = self._detect_delimiter()
        print("delimiter", delimiter)
        with open(self._file_path, newline='', encoding='utf-8') as f:
            lector = csv.DictReader(f, delimiter=delimiter)
            # Opcional: saltar la cabecera
            # next(lector)
            for fila in lector:
                yield fila


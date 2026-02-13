import csv
from pathlib import Path
from typing import Generator, Protocol
import pandas as pd

# TODO: añadir lector para json (puede cargarlo todo de una)
# TODO: testing de las clases


class Reader(Protocol):
    def read(self) -> Generator: ...

class CSVReader(Reader):
    # TODO: si da tiempo completar esta version sin pandas

    def __init__(self, file_path: Path, chunk_size: int):
        self._file_path = file_path
        self._chunk_size = chunk_size

    def _detect_delimiter(self):
        with open(self._file_path, newline='', encoding='utf-8') as f:
            sample = f.read(100)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            delimiter = dialect.delimiter
            return delimiter


    def read(self) -> Generator:
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
            while lector:
                # init chunk
                chunk = []
                chunk_size = 0
                # read chunk
                while chunk_size < self._chunk_size:
                    chunk.append(next(lector))
                    chunk_size += 1

                yield chunk

class CSVReaderPandas(Reader):
    # TODO: gestion de errores y documentacion
    def __init__(self, file_path: Path, chunk_size: int):
        self._file_path = file_path
        self._chunk_size = chunk_size

    def _detect_delimiter(self):
        with open(self._file_path, newline='', encoding='utf-8') as f:
            sample = f.read(100)
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            delimiter = dialect.delimiter
            return delimiter


    def read(self) -> Generator:
        delimiter = self._detect_delimiter()
        lector = pd.read_csv(self._file_path, chunksize=self._chunk_size, sep=delimiter)
        for chunk in lector:
            yield chunk

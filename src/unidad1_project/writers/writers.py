from pathlib import Path
from typing import Protocol
from pandas import DataFrame

class Writer(Protocol):
    def write(self, data_frame: DataFrame) -> None: ...

class WriterCsv(Writer):
    def __init__(self, file_path: Path):
        # TODO: logica comprobacion `file_path`
        #       - si el archivo ya existe
        #           - comprobar si esta vacio
        #               - si lo esta nada
        #               - si tiene contenido levantar error
        #       - ¿? revisar las columnas del archivo (numero, nombre...) ¿?
        self._file_path = file_path

    def write(self, data_frame: DataFrame) -> None:
        print(f"writing file... {self._file_path}")
        print(data_frame)

        # TODO: logica de guardado
        # Abrir archivo en modo append y añadir el contenido del df

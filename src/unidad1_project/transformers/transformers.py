from typing import Generator, Protocol, Any
import pandas as pd

# TODO: TransformerMissingThreshold
# TODO: TransformerNormalizeStrings (lo que se nos ocurra)
# TODO: TransformerImputeMissings (imputar valor para los missings a usar con TransformerMissingThreshold)
# TODO: TransformerDateFormatter(unificar y modificar formato de las fechas)

class Transformer(Protocol):
    def transform(self, data: pd.DataFrame) -> pd.DataFrame: ...

class TransformerMissing(Transformer):
    # TODO: logica de transformacion, eliminar las filas con datos perdidos
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

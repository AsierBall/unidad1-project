
from typing import Generator, Protocol, Any
import pandas as pd


class Transformer(Protocol):
    def transform(self, data: pd.DataFrame) -> pd.DataFrame: ...

class TransformerMissing(Transformer):
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

class TransformerMissingBis(Transformer):
    def __init__(self): ...

    # iterar el generador para devolver el data.frame
    def transform(self, chunk):
        # TODO: logica de transformacion
        # logica del orquestador
        
        for chunk in reader.reading_chunks():
            cleaned_chunk = transformer.transform(chunk)
            writer.write_data(cleaned_chunk)
        
    def transform_data(self): ...

    def transform_generator(self): ...

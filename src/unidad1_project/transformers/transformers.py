from typing import Generator, Protocol, Any
import pandas as pd

class Transformer(Protocol):
    def transform(self, data: pd.DataFrame) -> pd.DataFrame: ...

class TransformerMissing(Transformer):
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.dropna()

class TransformerMissingThreshold(Transformer):
    def __init__(self, threshold: float):
        self._threshold = threshold

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.dropna(thresh=self._threshold*len(data.columns))

class TransformerNormalizeStrings(Transformer):
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        for column in data.select_dtypes(include='str').columns:
            data[column] = data[column].str.strip().str.lower()
        return data


class TransformerImputeMissingsNumeric(Transformer):
    def __init__(self, strategy: str = 'mean'):
        self._strategy = strategy

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if self._strategy == 'mean':
            return data.fillna(data.mean(numeric_only=True))
        elif self._strategy == 'median':
            return data.fillna(data.median(numeric_only=True))
        elif self._strategy == 'mode':
            return data.fillna(data.mode().iloc[0])
        else:
            raise ValueError(f"Unknown imputation strategy: {self._strategy}")

class TransformerImputeMissingsString(Transformer):
    def __init__(self, strategy: str = 'mode', default_value: Any = None):
        self._strategy = strategy
        self._default_value = default_value

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if self._strategy == 'mode':
            return data.fillna(data.mode().iloc[0])
        if self._strategy == 'default':
            return data.fillna(self._default_value)
        else:
            raise ValueError(f"Unknown imputation strategy: {self._strategy}")

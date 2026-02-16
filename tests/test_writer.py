import pandas as pd
import pytest
from pathlib import Path

from unidad1_project import WriterCsv


def test_raises_value_error_if_columns_do_not_match(tmp_path: Path) -> None:
    file_path = tmp_path / "output.csv"
    writer = WriterCsv(file_path)

    df_initial = pd.DataFrame({
        "name": ["Alice"],
        "age": [30],
    })

    df_invalid = pd.DataFrame({
        "name": ["Bob"],
        "city": ["Madrid"],
    })

    # Primero creamos el archivo con las columnas originales
    writer.write(df_initial)

    # Ahora intentamos escribir columnas distintas
    with pytest.raises(ValueError):
        writer.write(df_invalid)

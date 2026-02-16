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


def test_creates_file_if_not_exists(tmp_path: Path) -> None:
    file_path = tmp_path / "output.csv"
    writer = WriterCsv(file_path)

    df = pd.DataFrame({"name": ["Alice"], "age": [30]})

    writer.write(df)

    assert file_path.exists()

def test_appends_rows(tmp_path: Path) -> None:
    file_path = tmp_path / "output.csv"
    writer = WriterCsv(file_path)

    df1 = pd.DataFrame({"name": ["Alice"], "age": [30]})
    df2 = pd.DataFrame({"name": ["Bob"], "age": [25]})

    writer.write(df1)
    writer.write(df2)

    result = pd.read_csv(file_path)

    assert len(result) == 2

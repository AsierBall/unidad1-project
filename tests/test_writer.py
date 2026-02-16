import pandas as pd
import pytest
from pathlib import Path

from unidad1_project import WriterCsv


class TestCSVWriter:
    def test_raises_value_error_if_columns_do_not_match(self,tmp_path: Path) -> None:
        """
        Verify that a ValueError is raised when attempting to append
        a DataFrame with columns that do not match the existing file.
        """
        file_path = tmp_path / "output.csv"
        writer = WriterCsv()

        df_initial = pd.DataFrame({
            "name": ["Alice"],
            "age": [30],
        })

        df_invalid = pd.DataFrame({
            "name": ["Bob"],
            "city": ["Madrid"],
        })

        # Primero creamos el archivo con las columnas originales
        writer.write(df_initial, file_path)

        # Ahora intentamos escribir columnas distintas
        with pytest.raises(ValueError):
            writer.write(df_invalid, file_path)


    def test_creates_file_if_not_exists(self,tmp_path: Path) -> None:
        """
        Verify that the CSV file is created if it does not exist.
        """
        file_path = tmp_path / "output.csv"
        writer = WriterCsv()

        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        writer.write(df, file_path)

        assert file_path.exists()

    def test_creates_parent_directory(self,tmp_path: Path) -> None:
        """
        Check that writing to a file in subfolders automatically
        creates any missing directories.
        """
        output_path = tmp_path / "folder" / "output.csv"
        writer = WriterCsv()

        df = pd.DataFrame({"name": ["Alice"], "age": [30]})

        writer.write(df, output_path)

        assert output_path.exists()



    def test_appends_rows(self, tmp_path: Path) -> None:
        file_path = tmp_path / "output.csv"
        writer = WriterCsv()

        df1 = pd.DataFrame({"name": ["Alice"], "age": [30]})
        df2 = pd.DataFrame({"name": ["Bob"], "age": [25]})

        writer.write(df1, file_path)
        writer.write(df2, file_path)

        result = pd.read_csv(file_path)

        assert len(result) == 2

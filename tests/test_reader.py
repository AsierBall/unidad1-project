import pytest
import pandas as pd
import json
from pathlib import Path
from typing import Generator

from unidad1_project import CSVReaderPandas, JSONReaderPandas
# --- Test data fixtures ---


@pytest.fixture
def csv_file_comma(tmp_path) -> Path:
    path = tmp_path / "test_comma.csv"
    content = "col1,col2\n1,a\n2,b\n3,c\n4,d\n5,e"
    path.write_text(content)
    return path


@pytest.fixture
def csv_file_semicolon(tmp_path) -> Path:
    path = tmp_path / "test_semi.csv"
    content = "col1;col2\n1;a\n2;b"
    path.write_text(content)
    return path


@pytest.fixture
def json_file_valid(tmp_path) -> Path:
    path = tmp_path / "test.json"
    data = [{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}]
    path.write_text(json.dumps(data))
    return path


# --- CSVReaderPandas tests ---


class TestCSVReaderPandas:
    def test_init_raises_error_on_invalid_chunk_size(self):
        with pytest.raises(ValueError, match="positive integer"):
            CSVReaderPandas(chunk_size=0)

    def test_detect_delimiter_comma(self, csv_file_comma):
        reader = CSVReaderPandas(chunk_size=2)
        assert reader._detect_delimiter(csv_file_comma) == ","

    def test_detect_delimiter_semicolon(self, csv_file_semicolon):
        reader = CSVReaderPandas(chunk_size=2)
        assert reader._detect_delimiter(csv_file_semicolon) == ";"

    def test_read_yields_correct_chunks(self, csv_file_comma):
        """5 total rows, chunk_size=2 -> should return 3 chunks (2, 2, 1)"""
        reader = CSVReaderPandas(chunk_size=2)
        chunks = list(reader.read(csv_file_comma))

        assert len(chunks) == 3
        assert isinstance(chunks[0], pd.DataFrame)
        assert len(chunks[0]) == 2
        assert len(chunks[2]) == 1

    def test_read_file_not_found(self):
        reader = CSVReaderPandas(chunk_size=5)
        with pytest.raises(FileNotFoundError):
            list(reader.read(Path("missing.csv")))

    def test_read_invalid_extension_warning(self, tmp_path, caplog):
        path = tmp_path / "data.txt"
        path.write_text("a,b\n1,2")
        reader = CSVReaderPandas(chunk_size=5)

        list(reader.read(path))
        assert "is not .csv" in caplog.text


# --- JSONReaderPandas tests ---


class TestJSONReaderPandas:
    def test_read_valid_json(self, json_file_valid):
        reader = JSONReaderPandas()
        generator = reader.read(json_file_valid)

        df = next(generator)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert df.iloc[0]["col2"] == "a"

    def test_read_malformed_json(self, tmp_path):
        path = tmp_path / "broken.json"
        path.write_text("{'invalid': 'json'}")

        reader = JSONReaderPandas()
        with pytest.raises(ValueError, match="not a valid JSON"):
            list(reader.read(path))

    def test_read_empty_file_warning(self, tmp_path, caplog):
        path = tmp_path / "empty.json"
        path.write_text("[]")

        reader = JSONReaderPandas()
        list(reader.read(path))
        assert "returned an empty DataFrame" in caplog.text

    def test_is_generator(self, json_file_valid):
        reader = JSONReaderPandas()
        gen = reader.read(json_file_valid)
        assert isinstance(gen, Generator)

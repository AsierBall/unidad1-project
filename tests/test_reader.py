import pytest
from pathlib import Path

from unidad1_project import CSVReaderPandas

def test_csvreader_instances_properly():
    csv_reader = CSVReaderPandas(Path("test"), 100)

    assert isinstance(csv_reader, CSVReaderPandas)

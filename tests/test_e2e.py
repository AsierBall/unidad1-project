import pytest
from pathlib import Path

from unidad1_project import Orchestrator, CSVReaderPandas, WriterCsv, \
    TransformerMissing, TransformerNormalizeStrings
from unidad1_project.readers.readers import JSONReaderPandas

@pytest.fixture
def csv_path():
    return Path(__file__).parent / "data" / "test.csv"


@pytest.fixture
def json_path():
    return Path(__file__).parent / "data" / "test.json"

def count_file_rows(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

class TestE2E:
    def test_csv_reader(self, csv_path, tmp_path):
        output_path = tmp_path / "out.csv"

        orchestrator = Orchestrator(
            reader = CSVReaderPandas(chunk_size=100),
            transformers = [
                TransformerMissing(),
                TransformerNormalizeStrings(),
            ],
            writer = WriterCsv()
            )

        orchestrator.run(csv_path, output_path)

        assert Path.is_file(output_path)

        num_rows_input = count_file_rows(csv_path)
        num_rows_output = count_file_rows(output_path)
        assert num_rows_input >= num_rows_output

    def test_json_reader(self, json_path, tmp_path):
        output_path = tmp_path / "out.json"

        orchestrator = Orchestrator(
            reader = JSONReaderPandas(),
            transformers = [
                TransformerMissing(),
                TransformerNormalizeStrings(),
            ],
            writer = WriterCsv()
            )

        orchestrator.run(json_path, output_path)

        assert Path.is_file(output_path)

        num_rows_input = count_file_rows(json_path)
        num_rows_output = count_file_rows(output_path)
        assert num_rows_input >= num_rows_output

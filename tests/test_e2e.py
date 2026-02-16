import pytest
from pathlib import Path
import json

from unidad1_project import Orchestrator, CSVReaderPandas, WriterCsv, WriterJson, \
    TransformerMissing, TransformerNormalizeStrings

@pytest.fixture
def csv_path():
    return Path(__file__).parent / "data" / "test.csv"


@pytest.fixture
def json_path():
    return Path(__file__).parent / "data" / "test.json"

def count_file_rows(file_path):
    with open(file_path, 'r', encoding="utf-8") as f:
        return sum(1 for _ in f)

def count_file_rows_json(file_path):
    with file_path.open("r", encoding="utf-8") as f:
            lines = f.readlines()
    
    return len([json.loads(line) for line in lines])

def count_file_rows_json_oneliner(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    return len(data)
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

    def test_json(self, json_path, tmp_path):
        output_path = tmp_path / "out.json"

        orchestrator = Orchestrator(
            reader = JSONReaderPandas(),
            transformers = [
                TransformerMissing(),
                TransformerNormalizeStrings(),
            ],
            writer = WriterJson()
            )

        orchestrator.run(json_path, output_path)

        assert Path.is_file(output_path)

        num_rows_input = count_file_rows_json_oneliner(json_path)
        num_rows_output = count_file_rows_json(output_path)
        assert num_rows_input >= num_rows_output



import pytest
import pandas as pd
from pathlib import Path
from unidad1_project import Orchestrator, Transformer, Reader, Writer

# --- Mocks ---

class MockReader(Reader):
    def read(self, file_path: Path):
        for chunk in range(10):
            yield chunk

class MockTransformer(Transformer):
    def transform(self, data):
        return data

class MockWriter(Writer):
    def write(self, data_frame: pd.DataFrame, file_path: Path):
        pass

# --- Fixtures ---

@pytest.fixture
def input_path(): return Path("in.txt")

@pytest.fixture
def output_path(): return Path("out.txt")

# --- Tests ---

class TestOrchestratorFlow:
    def test_run_execution(self, input_path, output_path, caplog):
        """Tests the pipeline execution and its logging"""
        reader = MockReader()
        transformer = MockTransformer()
        writer = MockWriter()

        orchestrator = Orchestrator(
            reader=reader,
            transformers=[transformer, transformer],
            writer=writer
        )

        orchestrator.run(input_path, output_path)

        assert "chunk 1 processed" in caplog.text

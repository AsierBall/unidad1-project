import pytest
from pathlib import Path
from unidad1_project import Orchestrator, Transformer, Reader, Writer# Ajusta la importación

# --- Mocks Manuales (Reutilizables) ---

class MockReader(Reader):
    def read(self, path):
        for chunk in range(10):
            yield chunk

class MockTransformer(Transformer):
    def transform(self, data):
        return data

class MockWriter(Writer):
    def write(self, path):
        pass

# --- Fixtures para inicializar el escenario ---

@pytest.fixture
def input_path(): return Path("in.txt")

@pytest.fixture
def output_path(): return Path("out.txt")

# --- Tests Atómicos ---

class TestOrchestratorFlow:
    def test_run_execution(self, input_path, output_path):
        reader = MockReader()
        transformer = MockTransformer()
        writer = MockWriter()

        orchestrator = Orchestrator(
            reader=reader,
            transformers=[transformer, transformer],
            writer=writer
        )

        orchestrator.run(input_path, output_path)

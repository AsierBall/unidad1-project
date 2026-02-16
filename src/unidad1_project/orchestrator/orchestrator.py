from ..readers import Reader
from ..transformers import Transformer
from ..writers import Writer
from pathlib import Path
from functools import reduce

class Orchestrator():
    def __init__(self, reader: Reader, transformers: list[Transformer], writer: Writer):
        self._reader = reader
        self._transformers = transformers
        self._writer = writer

    def run(self, input_file_path: Path, output_file_path: Path):
        for chunk in self._reader.read(input_file_path):
            cleaned_chunk = reduce(
                lambda data, transformer: transformer.transform(data),
                self._transformers,
                chunk
            )

            self._writer.write(cleaned_chunk, output_file_path)

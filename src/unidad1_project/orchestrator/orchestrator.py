from ..readers import Reader
from ..transformers import Transformer
from ..writers import Writer

class Orchestrator():
    def __init__(self, reader: Reader, transformer: Transformer, writer: Writer):
        self._reader = reader
        self._transformer = transformer
        self._writer = writer

    def run(self):
        for chunk in self._reader.read():
            cleaned_chunk = self._transformer.transform(chunk)
            self._writer.write(cleaned_chunk)

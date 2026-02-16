from ..readers import Reader
from ..transformers import Transformer
from ..writers import Writer
from pathlib import Path
from functools import reduce

class Orchestrator():
    """
    Orchestrator that executes a set of `Transformer` to a
    data stream coming from a `Reader` and writes it using `Writer`

    :param reader: reader with the read configuration
    :type reader: Reader
    :param transformers: a list of transformers that must be executed to the data
    :type transformers: list[Transformer]
    :param writer: a writer with the write configuration
    :type writer: Writer
    """
    def __init__(self, reader: Reader, transformers: list[Transformer], writer: Writer):
        self._reader = reader
        self._transformers = transformers
        self._writer = writer

<<<<<<< Updated upstream
    def run(self, input_file_path: Path, output_file_path: Path):
        """runs the data stream from reading to writing

        :param input_file_path: path to the input file
        :type input_file_path: Path
        :param output_file_path: path to the output file
        :type output_file_path: Path
        """
        for chunk in self._reader.read(input_file_path):
            cleaned_chunk = reduce(
                lambda data, transformer: transformer.transform(data),
                self._transformers,
                chunk
            )

            self._writer.write(cleaned_chunk, output_file_path)
=======
    def run(self, input_path, output_path):
        for chunk in self._reader.read(input_path):
            cleaned_chunk = self._transformer.transform(chunk)
            self._writer.write(cleaned_chunk, output_path)
>>>>>>> Stashed changes

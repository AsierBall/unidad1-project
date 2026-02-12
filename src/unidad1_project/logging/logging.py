from logging import basicConfig, FileHandler, StreamHandler
from pathlib import Path


class PipelineLogger():
    def __init__(self, log_file: Path, numeric_level, log_format):
        # Configure root logger to output to both file and stream
        self._logger = basicConfig(
            level=numeric_level,
            format=log_format,
            handlers=[
                FileHandler(log_file),
                StreamHandler()
            ]
        )

    def log(self, log_text: str) -> None: ...


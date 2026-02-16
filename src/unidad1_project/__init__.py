from pathlib import Path
from .readers import CSVReaderPandas, JSONReaderPandas, Reader
from .transformers import (Transformer,
    TransformerMissing,
    TransformerMissingThreshold,
    TransformerNormalizeStrings,
    TransformerImputeMissingsNumeric,
    TransformerImputeMissingsString,
    TransformerGroupByAggregate,
    TransformerSelectColumns,
    TransformerFilterRows,
    )
from .writers import WriterCsv, Writer
from .orchestrator import Orchestrator
from .logging import setup_logging

setup_logging(
    log_level="INFO",
    log_file=Path("logs/unidad1_project.log"),
    log_to_console=True
)

__all__ = [
    "Reader",
    "JSONReaderPandas",
    "CSVReaderPandas",
    "TransformerMissing",
    "WriterCsv",
    "Writer",
    "Orchestrator",
    "Transformer",
    "TransformerMissingThreshold",
    "TransformerNormalizeStrings",
    "TransformerImputeMissingsNumeric",
    "TransformerImputeMissingsString",
    "TransformerGroupByAggregate",
    "TransformerSelectColumns",
    "TransformerFilterRows",
]

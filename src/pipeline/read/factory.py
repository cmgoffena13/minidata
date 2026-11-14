from typing import ClassVar

from src.pipeline.read.base import BaseReader
from src.pipeline.read.csv_reader import CsvReader
from src.pipeline.read.db_reader import DbReader
from src.sources.base import SourceConfig


class ReaderFactory:
    readers: ClassVar[dict[str, type[BaseReader]]] = {
        "csv": CsvReader,
        "db": DbReader,
    }

    @classmethod
    def get_reader(cls, config: SourceConfig) -> BaseReader:
        reader_class = cls.readers.get(config.source_type)
        if not reader_class:
            raise ValueError(f"Reader for source type {config.source_type} not found")
        return reader_class(config)

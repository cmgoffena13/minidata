from typing import ClassVar

from src.pipeline.read.base import BaseReader
from src.sources.base import SourceConfig


class ReaderFactory:
    readers: ClassVar[dict[str, type[BaseReader]]] = {}

    @classmethod
    def get_reader(cls, config: SourceConfig) -> BaseReader:
        reader_class = cls.readers.get(config.source_type)
        if not reader_class:
            raise ValueError(f"Reader for source type {config.source_type} not found")
        return reader_class(config)

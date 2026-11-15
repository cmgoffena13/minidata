from abc import ABC, abstractmethod

from polars import LazyFrame

from src.pipeline.polars_utils import conform_to_schema, null_blank_strings
from src.sources.base import SourceConfig


class BaseReader(ABC):
    def __init__(self, config: SourceConfig):
        self.source_config = config

    def read(self) -> LazyFrame:
        frame = null_blank_strings(self._read_source())
        source_schema = self.source_config.source_schema
        return conform_to_schema(
            frame,
            source_schema.to_polars_schema(),
            source_schema.source_rename_map(),
        )

    @abstractmethod
    def _read_source(self) -> LazyFrame:
        pass

from abc import ABC, abstractmethod

from polars import DataFrame

from src.sources.base import SourceConfig


class BaseReader(ABC):
    def __init__(self, config: SourceConfig):
        self.source_config = config

    @abstractmethod
    def read(self) -> DataFrame:
        pass

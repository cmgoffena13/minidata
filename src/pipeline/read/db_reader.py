from polars import LazyFrame, read_database_uri

from src.pipeline.read.base import BaseReader
from src.sources.db_source import DBSourceConfig


class DbReader(BaseReader):
    def _read_source(self) -> LazyFrame:
        config = self.source_config
        if not isinstance(config, DBSourceConfig):
            raise TypeError("DbReader requires a DBSourceConfig")
        return read_database_uri(config.query, config.location).lazy()

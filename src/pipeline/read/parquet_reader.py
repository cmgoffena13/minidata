from polars import LazyFrame, scan_parquet

from src.pipeline.read.base import BaseReader
from src.sources.parquet_source import ParquetSourceConfig


class ParquetReader(BaseReader):
    def _read_source(self) -> LazyFrame:
        config = self.source_config
        if not isinstance(config, ParquetSourceConfig):
            raise TypeError("ParquetReader requires a ParquetSourceConfig")
        if config.location is None:
            raise ValueError("ParquetReader requires a file location")
        return scan_parquet(config.location, storage_options=config.storage_options)

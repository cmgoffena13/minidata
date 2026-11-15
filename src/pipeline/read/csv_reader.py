from polars import LazyFrame, scan_csv

from src.pipeline.read.base import BaseReader
from src.sources.csv_source import CSVSourceConfig


class CsvReader(BaseReader):
    def _read_source(self) -> LazyFrame:
        config = self.source_config
        if not isinstance(config, CSVSourceConfig):
            raise TypeError("CsvReader requires a CSVSourceConfig")
        if config.location is None:
            raise ValueError("CsvReader requires a file location")
        return scan_csv(config.location, storage_options=config.storage_options)

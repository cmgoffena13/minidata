from polars import LazyFrame, scan_csv

from src.pipeline.read.base import BaseReader
from src.sources.csv_source import CSVSourceConfig


class CsvReader(BaseReader):
    def read(self) -> LazyFrame:
        config = self.source_config
        if not isinstance(config, CSVSourceConfig):
            raise TypeError("CsvReader requires a CSVSourceConfig")
        return scan_csv(config.location, storage_options=config.storage_options)

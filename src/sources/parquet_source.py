from src.sources.base import SourceConfig


class ParquetSourceConfig(SourceConfig):
    source_type: str = "parquet"
    location: str
    storage_options: dict[str, str] | None = None

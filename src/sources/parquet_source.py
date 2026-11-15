from src.sources.file_source import FileSourceConfig


class ParquetSourceConfig(FileSourceConfig):
    source_type: str = "parquet"

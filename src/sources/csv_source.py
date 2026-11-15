from src.sources.file_source import FileSourceConfig


class CSVSourceConfig(FileSourceConfig):
    source_type: str = "csv"

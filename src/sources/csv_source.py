from src.sources.base import SourceConfig


class CSVSourceConfig(SourceConfig):
    source_type: str = "csv"
    location: str
    storage_options: dict[str, str] | None = None

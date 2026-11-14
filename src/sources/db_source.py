from src.sources.base import SourceConfig


class DBSourceConfig(SourceConfig):
    source_type: str = "db"
    location: str
    query: str

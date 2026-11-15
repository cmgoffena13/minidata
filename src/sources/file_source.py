from src.sources.base import SourceConfig


class FileSourceConfig(SourceConfig):
    directory: str
    pattern: str
    archive_directory: str
    storage_options: dict[str, str] | None = None
    location: str | None = None
    staging_name: str | None = None

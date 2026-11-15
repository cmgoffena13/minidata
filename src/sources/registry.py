from fnmatch import fnmatchcase

from pydantic import BaseModel, Field

from src.sources.base import SourceConfig


class SourceRegistry(BaseModel):
    sources: list[SourceConfig] = Field(default_factory=list)

    def add_sources(self, sources: list[SourceConfig]) -> None:
        self.sources.extend(sources)

    def get_sources(self) -> list[SourceConfig]:
        return self.sources

    def find_sources(self, pattern: str) -> list[SourceConfig]:
        return [source for source in self.sources if fnmatchcase(source.name, pattern)]


ALL_CONFIGS = SourceRegistry()

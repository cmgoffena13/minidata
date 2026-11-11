from src.sources.base import SourceConfig


class Processor:
    def __init__(self, master_registry: list[SourceConfig]):
        self.master_registry = master_registry

    def process(self) -> None:
        pass

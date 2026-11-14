from polars import LazyFrame

from src.pipeline.write.writer import DeltaWriter
from src.sources.base import SourceConfig


class PipelineRunner:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config

    def read(self) -> None:
        pass

    def validate(self) -> None:
        pass

    def write(self, frame: LazyFrame) -> str:
        return DeltaWriter(self.source_config).write(frame)

    def audit(self) -> None:
        pass

    def publish(self) -> None:
        pass

    def run(self) -> None:
        pass

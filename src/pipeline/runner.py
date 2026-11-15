from polars import LazyFrame

from src.pipeline.audit import StagingAuditor
from src.pipeline.publisher import Publisher
from src.pipeline.read.factory import ReaderFactory
from src.pipeline.target import create_target_table
from src.pipeline.write.writer import DeltaWriter
from src.sources.base import SourceConfig


class PipelineRunner:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config
        create_target_table(source_config)
        self.publisher = Publisher(source_config)

    def read(self) -> LazyFrame:
        return ReaderFactory.get_reader(self.source_config).read()

    def write(self, frame: LazyFrame) -> str:
        return DeltaWriter(self.source_config).write(frame=frame)

    def audit(self, staging_table_name: str) -> None:
        StagingAuditor(self.source_config).audit(staging_table_name)

    def publish(self, staging_table_name: str) -> None:
        self.publisher.publish(staging_table_name)

    def run(self) -> None:
        frame = self.read()
        staging_table_name = self.write(frame)
        self.audit(staging_table_name)
        self.publish(staging_table_name)

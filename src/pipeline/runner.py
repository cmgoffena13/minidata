from pathlib import Path

from deltalake import DeltaTable
from deltalake.fs import DeltaStorageHandler
from polars import LazyFrame
from pydantic import BaseModel, ConfigDict

from src.pipeline.audit.auditor import StagingAuditor
from src.pipeline.pipeline_utils import create_target_table
from src.pipeline.publish.publisher import Publisher
from src.pipeline.read.factory import ReaderFactory
from src.pipeline.write.writer import DeltaWriter
from src.sources.base import SourceConfig
from src.sources.file_source import FileSourceConfig
from src.utils import retry


class PipelineRunResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    success: bool
    source_name: str
    error: BaseException | None = None
    location: str | None = None


class PipelineRunner:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config
        create_target_table(source_config)
        self.publisher = Publisher(source_config)

    @retry()
    def read(self) -> LazyFrame:
        return ReaderFactory.get_reader(self.source_config).read()

    def write(self, frame: LazyFrame, staging_name: str | None = None) -> str:
        return DeltaWriter(self.source_config).write(frame=frame, staging_name=staging_name)

    @retry()
    def audit(self, staging_table_name: str) -> None:
        StagingAuditor(self.source_config).audit(staging_table_name)

    @retry()
    def publish(self, staging_table_name: str) -> None:
        self.publisher.publish(staging_table_name)

    @retry()
    def cleanup(self, staging_table_name: str) -> None:
        table = DeltaTable(staging_table_name)
        DeltaStorageHandler.from_table(table._table).delete_dir("")
        directory = Path(staging_table_name)
        if "://" not in staging_table_name and directory.is_dir():
            for path in sorted(directory.rglob("*"), reverse=True):
                if path.is_dir():
                    path.rmdir()
            directory.rmdir()

    def run(self) -> PipelineRunResult:
        error: BaseException | None = None
        location = None
        try:
            staging_name = None
            if isinstance(self.source_config, FileSourceConfig):
                staging_name = self.source_config.staging_name
                location = self.source_config.location
            frame = self.read()
            staging_table_name = self.write(frame, staging_name=staging_name)
            self.audit(staging_table_name)
            self.publish(staging_table_name)
            self.cleanup(staging_table_name)
        except Exception as caught:  # noqa: BLE001
            error = caught
        return PipelineRunResult(
            success=error is None,
            source_name=self.source_config.name,
            error=error,
            location=location,
        )

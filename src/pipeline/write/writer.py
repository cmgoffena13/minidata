from polars import LazyFrame

from src.pipeline.pipeline_utils import warehouse_directory
from src.sources.base import SourceConfig


class DeltaWriter:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config

    def write(self, frame: LazyFrame) -> str:
        staging_table_name = self._staging_target()
        frame.sink_delta(
            staging_table_name,
            mode="overwrite",
            delta_write_options=self._write_options(),
        )
        return staging_table_name

    def _staging_target(self) -> str:
        return f"{warehouse_directory()}/staging_{self.source_config.name}"

    def _write_options(self) -> dict[str, str | list[str]]:
        options: dict[str, str | list[str]] = {"schema_mode": "overwrite"}
        if self.source_config.partition_by:
            options["partition_by"] = self.source_config.partition_by
        return options

from polars import LazyFrame

from src.pipeline.pipeline_utils import warehouse_directory
from src.pipeline.polars_utils import add_etl_row_hash
from src.sources.base import SourceConfig


class DeltaWriter:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config

    def write(self, frame: LazyFrame, staging_name: str | None = None) -> str:
        staging_table_name = self._staging_target(staging_name)
        add_etl_row_hash(frame).sink_delta(
            staging_table_name,
            mode="overwrite",
            delta_write_options=self._write_options(),
        )
        return staging_table_name

    def _staging_target(self, staging_name: str | None = None) -> str:
        name = self.source_config.name if staging_name is None else staging_name
        return f"{warehouse_directory()}/staging_{name}"

    def _write_options(self) -> dict[str, str | list[str]]:
        options: dict[str, str | list[str]] = {"schema_mode": "overwrite"}
        if self.source_config.partition_by:
            options["partition_by"] = self.source_config.partition_by
        return options

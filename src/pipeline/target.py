from polars import DataFrame

from src.pipeline.pipeline_utils import warehouse_directory
from src.sources.base import SourceConfig


def target_table_name(source_config: SourceConfig) -> str:
    return f"{warehouse_directory()}/{source_config.name}"


def create_target_table(source_config: SourceConfig) -> str:
    table_name = target_table_name(source_config)
    frame = DataFrame(schema=source_config.source_schema.to_polars_schema())
    partitions = source_config.partition_by
    write_options = {"partition_by": partitions} if partitions else None
    frame.write_delta(table_name, mode="ignore", delta_write_options=write_options)
    return table_name

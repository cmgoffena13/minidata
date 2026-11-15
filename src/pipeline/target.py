from polars import DataFrame

from src.settings import config
from src.sources.base import SourceConfig


def target_table_name(source_config: SourceConfig) -> str:
    warehouse = config.DATAWAREHOUSE_URL
    if warehouse is None:
        raise ValueError("DATAWAREHOUSE_URL is not set")
    return f"{warehouse.rstrip('/')}/{source_config.name}"


def create_target_table(source_config: SourceConfig) -> str:
    table_name = target_table_name(source_config)
    frame = DataFrame(schema=source_config.source_schema.to_polars_schema())
    partitions = source_config.partition_by
    write_options = {"partition_by": partitions} if partitions else None
    frame.write_delta(table_name, mode="ignore", delta_write_options=write_options)
    return table_name

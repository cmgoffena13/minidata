from pathlib import Path

from polars import DataFrame, String

from src.settings import config
from src.sources.base import SourceConfig


def warehouse_directory() -> str:
    warehouse = config.DATAWAREHOUSE_URL
    if warehouse is None:
        raise ValueError("DATAWAREHOUSE_URL is not set")
    directory = warehouse.rstrip("/")
    if "://" not in directory:
        Path(directory).mkdir(parents=True, exist_ok=True)
    return directory


def target_table_name(source_config: SourceConfig) -> str:
    return f"{warehouse_directory()}/{source_config.name}"


def create_target_table(source_config: SourceConfig) -> str:
    table_name = target_table_name(source_config)
    schema = dict(source_config.source_schema.to_polars_schema())
    schema["etl_row_hash"] = String
    frame = DataFrame(schema=schema)
    partitions = source_config.partition_by
    write_options = {"partition_by": partitions} if partitions else None
    frame.write_delta(table_name, mode="ignore", delta_write_options=write_options)
    return table_name


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'

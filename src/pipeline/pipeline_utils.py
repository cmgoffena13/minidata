import time
from functools import wraps
from pathlib import Path

from polars import Boolean, DataFrame, Date, Datetime, String

from src.settings import config
from src.sources.base import SourceConfig


def retry(attempts: int = 3, delay: float = 0.25, backoff: float = 2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wait = delay
            for index in range(attempts):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if index == attempts - 1:
                        raise
                    time.sleep(wait)
                    wait *= backoff
            raise ValueError("Attempts must be greater than 0")

        return wrapper

    return decorator


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
    schema["etl_created_on"] = Datetime(time_zone="UTC")
    if source_config.publish_mode == "mirror":
        schema["etl_updated_on"] = Datetime(time_zone="UTC")
        schema["is_source_system_active"] = Boolean
    if source_config.publish_mode == "snapshot":
        schema["snapshot_date"] = Date
    frame = DataFrame(schema=schema)
    partitions = source_config.partition_by
    write_options = {"partition_by": partitions} if partitions else None
    frame.write_delta(table_name, mode="ignore", delta_write_options=write_options)
    return table_name


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'

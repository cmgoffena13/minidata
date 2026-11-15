from pathlib import Path

from src.settings import config


def warehouse_directory() -> str:
    warehouse = config.DATAWAREHOUSE_URL
    if warehouse is None:
        raise ValueError("DATAWAREHOUSE_URL is not set")
    directory = warehouse.rstrip("/")
    if "://" not in directory:
        Path(directory).mkdir(parents=True, exist_ok=True)
    return directory


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'

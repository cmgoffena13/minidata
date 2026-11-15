from pathlib import Path

import duckdb
from polars import DataFrame

from src.pipeline.pipeline_utils import quote_identifier, warehouse_directory


def _delta_view_sql(schema: str, table: str, location: str) -> str:
    path = location.replace("'", "''")
    return (
        f"CREATE OR REPLACE VIEW {schema}.{table} AS "  # noqa: S608
        f"SELECT * FROM delta_scan('{path}')"
    )


def query(sql: str) -> DataFrame:
    warehouse = warehouse_directory()
    if "://" in warehouse:
        raise ValueError("Adhoc query supports local warehouse paths only")
    root = Path(warehouse)
    schema = quote_identifier(root.name)
    with duckdb.connect() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for path in sorted(root.iterdir()):
            if path.is_dir() and (path / "_delta_log").is_dir():
                table = quote_identifier(path.name)
                connection.execute(_delta_view_sql(schema, table, str(path)))
        return DataFrame(connection.sql(sql).arrow())

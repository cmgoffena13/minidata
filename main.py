from pathlib import Path

import duckdb

from src.logging_conf import setup_logging
from src.pipeline.target import target_table_name
from src.processor import Processor
from src.sources.base import SourceConfig
from src.sources.csv_source import CSVSourceConfig
from src.sources.registry import SourceRegistry
from src.tests.fixtures.widgets import WidgetSchema

ROOT = Path(__file__).resolve().parent


def select_target(source: SourceConfig) -> list[tuple[object, ...]]:
    with duckdb.connect() as connection:
        return list(
            connection.sql(
                "SELECT * FROM delta_scan(?)",
                params=[target_table_name(source)],
            ).fetchall()
        )


def main() -> None:
    setup_logging()
    source = CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        location=str(ROOT / "src/tests/fixtures/files/widgets.csv"),
    )
    registry = SourceRegistry()
    registry.add_sources([source])
    Processor(registry).process()
    print(select_target(source))


if __name__ == "__main__":
    main()

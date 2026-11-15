import duckdb

from src.pipeline.pipeline_utils import quote_identifier
from src.sources.base import SourceConfig


class StagingAuditor:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config

    def audit(self, staging_table_name: str) -> None:
        duplicates = self._duplicate_rows(staging_table_name)
        if duplicates:
            grain = self.source_config.grain
            raise ValueError(f"Duplicate grain {grain} in {staging_table_name}: {duplicates}")

    def _duplicate_rows(self, staging_table_name: str) -> list[tuple[object, ...]]:
        grain = self.source_config.grain
        if not grain:
            raise ValueError("Grain is empty")
        columns = ", ".join(quote_identifier(column) for column in grain)
        with duckdb.connect() as connection:
            relation = connection.sql("SELECT * FROM delta_scan(?)", params=[staging_table_name])
            grouped = relation.aggregate(f"{columns}, count(*) AS duplicate_count", columns)
            rows = grouped.filter("duplicate_count > 1").fetchall()
        return list(rows)

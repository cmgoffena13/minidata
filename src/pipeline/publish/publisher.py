from polars import scan_delta

from src.pipeline.pipeline_utils import quote_identifier, target_table_name
from src.sources.base import SourceConfig


class Publisher:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config
        self.target_table_name = target_table_name(source_config)

    def publish(self, staging_table_name: str) -> None:
        grain = self.source_config.grain
        if not grain:
            raise ValueError("Grain is empty")
        predicate = " AND ".join(
            f"s.{quote_identifier(column)} = t.{quote_identifier(column)}" for column in grain
        )
        merger = scan_delta(staging_table_name).sink_delta(
            self.target_table_name,
            mode="merge",
            delta_merge_options={
                "predicate": predicate,
                "source_alias": "s",
                "target_alias": "t",
            },
        )
        if merger is None:
            raise RuntimeError("Delta merge was not started")
        assignments = self._source_assignments()
        hash_changed = (
            f"s.{quote_identifier('etl_row_hash')} != t.{quote_identifier('etl_row_hash')}"
        )
        merger.when_matched_update(
            updates={**assignments, "etl_updated_on": "current_timestamp()"},
            predicate=hash_changed,
        ).when_not_matched_insert(
            updates={**assignments, "etl_created_on": "current_timestamp()"},
        ).execute()

    def _source_assignments(self) -> dict[str, str]:
        columns = [*self.source_config.source_schema.to_polars_schema(), "etl_row_hash"]
        return {column: f"s.{quote_identifier(column)}" for column in columns}

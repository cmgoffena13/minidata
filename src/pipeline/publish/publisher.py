from datetime import UTC, datetime

from polars import scan_delta

from src.pipeline.pipeline_utils import managed_etl_columns, quote_identifier, target_table_name
from src.sources.base import SourceConfig


class Publisher:
    def __init__(self, source_config: SourceConfig) -> None:
        self.source_config = source_config
        self.target_table_name = target_table_name(source_config)

    def publish(self, staging_table_name: str) -> None:
        if self.source_config.publish_mode == "snapshot":
            self._publish_snapshot(staging_table_name)
        else:
            self._publish_mirror(staging_table_name)

    def _publish_mirror(self, staging_table_name: str) -> None:
        grain = self.source_config.grain
        if not grain:
            raise ValueError("Grain is empty")
        predicate = " AND ".join(
            f"s.{quote_identifier(column)} = t.{quote_identifier(column)}" for column in grain
        )
        merger = self._start_merge(staging_table_name, predicate)
        assignments = self._source_assignments(staging_table_name)
        active_column = "is_source_system_active"
        quoted = f"t.{quote_identifier(active_column)}"
        hash_changed = (
            f"s.{quote_identifier('etl_row_hash')} != t.{quote_identifier('etl_row_hash')}"
        )
        merger.when_matched_update(
            updates={
                **assignments,
                "etl_updated_on": "current_timestamp()",
                active_column: "true",
            },
            predicate=f"({hash_changed}) OR {quoted} = false",
        ).when_not_matched_insert(
            updates={
                **assignments,
                "etl_created_on": "current_timestamp()",
                active_column: "true",
            },
        ).when_not_matched_by_source_update(
            updates={"etl_updated_on": "current_timestamp()", active_column: "false"},
            predicate=f"{quoted} = true",
        ).execute()

    def _publish_snapshot(self, staging_table_name: str) -> None:
        snapshot_date = datetime.now(UTC).date().isoformat()
        dated = f"t.{quote_identifier('snapshot_date')} = DATE '{snapshot_date}'"
        merger = self._start_merge(staging_table_name, "false")
        assignments = self._source_assignments(staging_table_name)
        assignments["snapshot_date"] = f"DATE '{snapshot_date}'"
        merger.when_not_matched_by_source_delete(predicate=dated).when_not_matched_insert(
            updates={**assignments, "etl_created_on": "current_timestamp()"},
        ).execute()

    def _start_merge(self, staging_table_name: str, predicate: str):
        merger = scan_delta(staging_table_name).sink_delta(
            self.target_table_name,
            mode="merge",
            delta_merge_options={
                "predicate": predicate,
                "source_alias": "s",
                "target_alias": "t",
                "merge_schema": True,
            },
        )
        if merger is None:
            raise RuntimeError("Delta merge was not started")
        return merger

    def _source_assignments(self, staging_table_name: str) -> dict[str, str]:
        managed = set(managed_etl_columns(self.source_config.publish_mode))
        columns = [
            name
            for name in scan_delta(staging_table_name).collect_schema().names()
            if name not in managed
        ]
        return {column: f"s.{quote_identifier(column)}" for column in columns}

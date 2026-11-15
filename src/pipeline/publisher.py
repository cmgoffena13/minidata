from polars import scan_delta

from src.pipeline.pipeline_utils import quote_identifier
from src.pipeline.target import target_table_name
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
        merger.when_matched_update_all().when_not_matched_insert_all().execute()

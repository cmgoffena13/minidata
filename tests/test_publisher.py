from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable

from main import select_target
from src.pipeline.pipeline_utils import create_target_table
from src.pipeline.polars_utils import add_etl_row_hash
from src.pipeline.publish.publisher import Publisher
from src.pipeline.runner import PipelineRunner
from src.settings import config
from src.sources.csv_source import CSVSourceConfig
from tests.fixtures.widgets import WidgetSchema


def test_publisher_does_not_create_the_target(widget_source, warehouse) -> None:
    Publisher(widget_source)
    assert not (warehouse / "widgets").exists()
    create_target_table(widget_source)
    assert pl.read_delta(f"{warehouse}/widgets").is_empty()


def test_run_loads_the_fixture_csv(warehouse) -> None:
    location = Path(__file__).resolve().parent / "fixtures" / "files" / "widgets.csv"
    source = CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        location=str(location),
    )
    PipelineRunner(source).run()
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.height == 3
    assert not DeltaTable.is_deltatable(f"{warehouse}/staging_widgets")
    assert not (warehouse / "staging_widgets").exists()


def test_runner_creates_the_bronze_warehouse(widget_source, tmp_path, monkeypatch) -> None:
    warehouse = tmp_path / "datawarehouse" / "bronze"
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", str(warehouse))
    PipelineRunner(widget_source)
    assert warehouse.is_dir()


def test_runner_creates_empty_target(widget_source, warehouse) -> None:
    PipelineRunner(widget_source)
    target = pl.read_delta(f"{warehouse}/widgets")
    assert target.is_empty()
    assert target.columns == [
        "widget_id",
        "color",
        "etl_row_hash",
        "etl_created_on",
        "etl_updated_on",
    ]
    assert target.schema["etl_created_on"] == pl.Datetime(time_zone="UTC")
    assert target.schema["etl_updated_on"] == pl.Datetime(time_zone="UTC")
    PipelineRunner(widget_source)
    assert pl.read_delta(f"{warehouse}/widgets").is_empty()


def test_select_target_reads_the_published_rows(widget_source, widget_frame, warehouse) -> None:
    runner = PipelineRunner(widget_source)
    runner.publish(runner.write(widget_frame))
    expected = add_etl_row_hash(widget_frame).collect().sort("widget_id")
    rows = sorted(select_target(widget_source))
    assert [row[:3] for row in rows] == list(expected.iter_rows())
    assert all(created is not None and updated is None for _, _, _, created, updated in rows)
    assert (warehouse / "widgets").is_dir()


def test_publish_stamps_created_on_and_updates_changed_rows(
    widget_source,
    widget_frame,
    warehouse,
) -> None:
    runner = PipelineRunner(widget_source)
    runner.publish(runner.write(widget_frame))
    first = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    runner.publish(runner.write(widget_frame))
    repeated = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert first["etl_created_on"].null_count() == 0
    assert repeated.equals(first)
    runner.publish(runner.write(pl.LazyFrame({"widget_id": [2, 4], "color": ["green", "gold"]})))
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    changed = published.filter(pl.col("widget_id") == 2)
    inserted = published.filter(pl.col("widget_id") == 4)
    unchanged = published.filter(pl.col("widget_id").is_in([1, 3]))
    assert changed["etl_updated_on"].null_count() == 0
    assert changed["etl_created_on"].equals(
        first.filter(pl.col("widget_id") == 2)["etl_created_on"]
    )
    assert inserted["etl_created_on"].null_count() == 0
    assert inserted["etl_updated_on"].null_count() == 1
    assert unchanged["etl_updated_on"].null_count() == unchanged.height
    assert unchanged["etl_created_on"].equals(
        first.filter(pl.col("widget_id").is_in([1, 3]))["etl_created_on"]
    )


def test_publish_merges_staging_into_target(widget_source, widget_frame, warehouse) -> None:
    runner = PipelineRunner(widget_source)
    staging_table_name = runner.write(widget_frame)
    runner.publish(staging_table_name)
    assert DeltaTable.is_deltatable(staging_table_name)
    runner.cleanup(staging_table_name)
    assert not DeltaTable.is_deltatable(staging_table_name)
    assert not Path(staging_table_name).exists()
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.drop("etl_row_hash", "etl_created_on", "etl_updated_on").equals(
        widget_frame.collect().sort("widget_id")
    )
    assert published["etl_row_hash"].equals(
        add_etl_row_hash(widget_frame).collect().sort("widget_id")["etl_row_hash"]
    )
    update = pl.LazyFrame({"widget_id": [2, 4], "color": ["green", "gold"]})
    staging_table_name = runner.write(update)
    runner.publish(staging_table_name)
    runner.cleanup(staging_table_name)
    assert not DeltaTable.is_deltatable(staging_table_name)
    assert not Path(staging_table_name).exists()
    expected = pl.DataFrame({"widget_id": [1, 2, 3, 4], "color": ["red", "green", "red", "gold"]})
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.drop("etl_row_hash", "etl_created_on", "etl_updated_on").equals(
        expected.sort("widget_id")
    )
    assert published["etl_row_hash"].equals(
        add_etl_row_hash(expected.lazy()).collect().sort("widget_id")["etl_row_hash"]
    )


def test_publish_rejects_empty_grain(widget_source, widget_frame, warehouse) -> None:
    source = widget_source.model_copy(update={"grain": []})
    runner = PipelineRunner(source)
    staging_table_name = runner.write(widget_frame)
    with pytest.raises(ValueError, match="Grain is empty"):
        runner.publish(staging_table_name)
    assert DeltaTable.is_deltatable(staging_table_name)
    assert (warehouse / "widgets").is_dir()


def test_publisher_requires_warehouse(widget_source, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", None)
    with pytest.raises(ValueError, match="DATAWAREHOUSE_URL"):
        PipelineRunner(widget_source)

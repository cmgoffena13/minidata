from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable

from src.pipeline.publisher import Publisher
from src.pipeline.runner import PipelineRunner
from src.pipeline.target import create_target_table
from src.settings import config
from src.sources.csv_source import CSVSourceConfig
from src.tests.fixtures.widgets import WidgetSchema


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
    assert target.columns == ["widget_id", "color"]
    PipelineRunner(widget_source)
    assert pl.read_delta(f"{warehouse}/widgets").is_empty()


def test_publish_merges_staging_into_target(widget_source, widget_frame, warehouse) -> None:
    runner = PipelineRunner(widget_source)
    staging_table_name = runner.write(widget_frame)
    runner.publish(staging_table_name)
    assert DeltaTable.is_deltatable(staging_table_name)
    runner.cleanup(staging_table_name)
    assert not DeltaTable.is_deltatable(staging_table_name)
    assert not Path(staging_table_name).exists()
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.equals(widget_frame.collect().sort("widget_id"))
    update = pl.LazyFrame({"widget_id": [2, 4], "color": ["green", "gold"]})
    staging_table_name = runner.write(update)
    runner.publish(staging_table_name)
    runner.cleanup(staging_table_name)
    assert not DeltaTable.is_deltatable(staging_table_name)
    assert not Path(staging_table_name).exists()
    expected = pl.DataFrame({"widget_id": [1, 2, 3, 4], "color": ["red", "green", "red", "gold"]})
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.equals(expected.sort("widget_id"))


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

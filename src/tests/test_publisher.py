import polars as pl
import pytest

from src.pipeline.publisher import Publisher
from src.pipeline.runner import PipelineRunner
from src.pipeline.target import create_target_table
from src.settings import config


def test_publisher_does_not_create_the_target(widget_source, warehouse) -> None:
    Publisher(widget_source)
    assert not (warehouse / "widgets").exists()
    create_target_table(widget_source)
    assert pl.read_delta(f"{warehouse}/widgets").is_empty()


def test_runner_creates_empty_target(widget_source, warehouse) -> None:
    PipelineRunner(widget_source)
    target = pl.read_delta(f"{warehouse}/widgets")
    assert target.is_empty()
    assert target.columns == ["widget_id", "color"]
    PipelineRunner(widget_source)
    assert pl.read_delta(f"{warehouse}/widgets").is_empty()


def test_publish_merges_staging_into_target(widget_source, widget_frame, warehouse) -> None:
    runner = PipelineRunner(widget_source)
    runner.publish(runner.write(widget_frame))
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.equals(widget_frame.collect().sort("widget_id"))
    update = pl.LazyFrame({"widget_id": [2, 4], "color": ["green", "gold"]})
    runner.publish(runner.write(update))
    expected = pl.DataFrame({"widget_id": [1, 2, 3, 4], "color": ["red", "green", "red", "gold"]})
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.equals(expected.sort("widget_id"))


def test_publish_rejects_empty_grain(widget_source, warehouse) -> None:
    source = widget_source.model_copy(update={"grain": []})
    runner = PipelineRunner(source)
    with pytest.raises(ValueError, match="Grain is empty"):
        runner.publish(f"{warehouse}/staging/widgets")
    assert (warehouse / "widgets").is_dir()


def test_publisher_requires_warehouse(widget_source, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", None)
    with pytest.raises(ValueError, match="DATAWAREHOUSE_URL"):
        PipelineRunner(widget_source)

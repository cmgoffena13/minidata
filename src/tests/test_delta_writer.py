from pathlib import Path

import polars as pl
import pytest

from src.pipeline.runner import PipelineRunner
from src.pipeline.write.writer import DeltaWriter
from src.settings import config


def test_runner_writes_lazy_frame_to_staging_table(
    widget_source,
    widget_frame,
    warehouse: Path,
) -> None:
    path = PipelineRunner(widget_source).write(widget_frame)
    written = pl.read_delta(path).sort("widget_id")
    assert path == f"{warehouse}/staging_widgets"
    assert written.equals(widget_frame.collect().sort("widget_id"))


def test_runner_overwrites_staging_table(widget_source, widget_frame, warehouse: Path) -> None:
    runner = PipelineRunner(widget_source)
    runner.write(widget_frame)
    replacement = pl.LazyFrame({"widget_id": [9], "color": ["green"]})
    path = runner.write(replacement)
    assert pl.read_delta(path).equals(replacement.collect())
    assert path.startswith(str(warehouse))


def test_writer_partitions_staging_table(widget_source, widget_frame, warehouse: Path) -> None:
    source = widget_source.model_copy(update={"partition_by": ["color"]})
    path = DeltaWriter(source).write(widget_frame)
    table = Path(path)
    assert table == warehouse / "staging_widgets"
    assert (table / "color=red").is_dir()
    assert (table / "color=blue").is_dir()


def test_writer_requires_warehouse_url(
    widget_source,
    widget_frame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", None)
    with pytest.raises(ValueError, match="DATAWAREHOUSE_URL"):
        DeltaWriter(widget_source).write(widget_frame)

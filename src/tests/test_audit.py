import polars as pl
import pytest

from src.pipeline.runner import PipelineRunner


def test_audit_accepts_unique_grain(widget_source, widget_frame, warehouse) -> None:
    runner = PipelineRunner(widget_source)
    staging_table_name = runner.write(widget_frame)
    runner.audit(staging_table_name)
    assert staging_table_name.startswith(str(warehouse))


def test_audit_rejects_duplicate_grain(widget_source, warehouse) -> None:
    runner = PipelineRunner(widget_source)
    frame = pl.LazyFrame({"widget_id": [1, 1], "color": ["red", "blue"]})
    staging_table_name = runner.write(frame)
    with pytest.raises(ValueError, match="Duplicate grain"):
        runner.audit(staging_table_name)
    assert staging_table_name.startswith(str(warehouse))


def test_audit_uses_every_grain_column(widget_source, warehouse) -> None:
    source = widget_source.model_copy(update={"grain": ["widget_id", "color"]})
    runner = PipelineRunner(source)
    frame = pl.LazyFrame({"widget_id": [1, 1], "color": ["red", "blue"]})
    staging_table_name = runner.write(frame)
    runner.audit(staging_table_name)
    assert staging_table_name.startswith(str(warehouse))


def test_audit_ignores_overwritten_files(widget_source, widget_frame, warehouse) -> None:
    runner = PipelineRunner(widget_source)
    duplicate = pl.LazyFrame({"widget_id": [1, 1], "color": ["red", "blue"]})
    staging_table_name = runner.write(duplicate)
    runner.write(widget_frame)
    runner.audit(staging_table_name)
    assert staging_table_name.startswith(str(warehouse))


def test_audit_rejects_empty_grain(widget_source, warehouse) -> None:
    source = widget_source.model_copy(update={"grain": []})
    with pytest.raises(ValueError, match="Grain is empty"):
        PipelineRunner(source).audit("staging/widgets")
    assert (warehouse / "widgets").is_dir()

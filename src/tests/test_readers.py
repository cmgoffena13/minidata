import sqlite3
from pathlib import Path

import polars as pl
import pytest

from src.pipeline.read.csv_reader import CsvReader
from src.pipeline.read.db_reader import DbReader
from src.pipeline.runner import PipelineRunner
from src.sources.base import SourceConfig
from src.sources.csv_source import CSVSourceConfig
from src.sources.db_source import DBSourceConfig
from src.tests.fixtures.widgets import WIDGET_ROWS, WidgetSchema


def test_runner_reads_csv_location(tmp_path: Path, widget_frame: pl.LazyFrame) -> None:
    location = tmp_path / "widgets.csv"
    widget_frame.collect().write_csv(location)
    source = CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        location=str(location),
        storage_options={"aws_region": "us-east-1"},
    )
    frame = PipelineRunner(source).read()
    written = frame.collect().sort("widget_id")
    assert written.equals(widget_frame.collect().sort("widget_id"))


def test_runner_reads_database_location(tmp_path: Path, widget_frame: pl.LazyFrame) -> None:
    database = tmp_path / "widgets.db"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE widgets (widget_id INTEGER, color TEXT)")
    connection.executemany(
        "INSERT INTO widgets (widget_id, color) VALUES (?, ?)",
        list(zip(WIDGET_ROWS["widget_id"], WIDGET_ROWS["color"], strict=True)),
    )
    connection.commit()
    connection.close()
    source = DBSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        location=f"sqlite:///{database}",
        query="SELECT widget_id, color FROM widgets",
    )
    frame = PipelineRunner(source).read()
    written = frame.collect().sort("widget_id")
    assert written.equals(widget_frame.collect().sort("widget_id"))


def test_readers_reject_plain_source_config(widget_source: SourceConfig) -> None:
    with pytest.raises(TypeError, match="CSVSourceConfig"):
        CsvReader(widget_source).read()
    with pytest.raises(TypeError, match="DBSourceConfig"):
        DbReader(widget_source).read()

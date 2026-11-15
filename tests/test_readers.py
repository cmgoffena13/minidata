import sqlite3
from pathlib import Path

import polars as pl
import pytest

from src.exceptions import SchemaConformanceError
from src.pipeline.read.csv_reader import CsvReader
from src.pipeline.read.db_reader import DbReader
from src.pipeline.read.parquet_reader import ParquetReader
from src.pipeline.runner import PipelineRunner
from src.sources.base import SourceConfig
from src.sources.csv_source import CSVSourceConfig
from src.sources.db_source import DBSourceConfig
from src.sources.parquet_source import ParquetSourceConfig
from tests.fixtures.widgets import WIDGET_ROWS, AliasedWidgetSchema, WidgetSchema


def test_runner_reads_csv_location(
    tmp_path: Path,
    widget_frame: pl.LazyFrame,
    warehouse: Path,
) -> None:
    location = tmp_path / "widgets.csv"
    widget_frame.collect().write_csv(location)
    source = CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        directory=str(tmp_path),
        pattern="*.csv",
        archive_directory=str(tmp_path / "archive"),
        location=str(location),
        storage_options={"aws_region": "us-east-1"},
    )
    frame = PipelineRunner(source).read()
    written = frame.collect().sort("widget_id")
    assert written.equals(widget_frame.collect().sort("widget_id"))
    assert (warehouse / "widgets").is_dir()


def test_runner_reads_parquet_location(
    tmp_path: Path,
    widget_frame: pl.LazyFrame,
    warehouse: Path,
) -> None:
    location = tmp_path / "widgets.parquet"
    widget_frame.collect().write_parquet(location)
    source = ParquetSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        directory=str(tmp_path),
        pattern="*.parquet",
        archive_directory=str(tmp_path / "archive"),
        location=str(location),
        storage_options={"aws_region": "us-east-1"},
    )
    frame = PipelineRunner(source).read()
    written = frame.collect().sort("widget_id")
    assert written.equals(widget_frame.collect().sort("widget_id"))
    assert (warehouse / "widgets").is_dir()


def test_runner_reads_database_location(
    tmp_path: Path,
    widget_frame: pl.LazyFrame,
    warehouse: Path,
) -> None:
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
    assert (warehouse / "widgets").is_dir()


def test_readers_reject_plain_source_config(widget_source: SourceConfig) -> None:
    with pytest.raises(TypeError, match="CSVSourceConfig"):
        CsvReader(widget_source).read()
    with pytest.raises(TypeError, match="DBSourceConfig"):
        DbReader(widget_source).read()
    with pytest.raises(TypeError, match="ParquetSourceConfig"):
        ParquetReader(widget_source).read()


def test_csv_reader_renames_aliased_columns(tmp_path: Path) -> None:
    location = tmp_path / "widgets.csv"
    pl.DataFrame({"Widget ID": [1], "Purchase Date": ["2024-01-02"], "color": ["red"]}).write_csv(
        location
    )
    source = CSVSourceConfig(
        name="widgets",
        source_schema=AliasedWidgetSchema,
        grain=["widget_id"],
        directory=str(tmp_path),
        pattern="*.csv",
        archive_directory=str(tmp_path / "archive"),
        location=str(location),
    )
    written = CsvReader(source).read().collect()
    assert written.columns == ["widget_id", "purchase_date", "color"]
    assert written.to_dicts() == [{"widget_id": 1, "purchase_date": "2024-01-02", "color": "red"}]


def test_csv_reader_rejects_files_missing_schema_columns(tmp_path: Path) -> None:
    location = tmp_path / "widgets.csv"
    pl.DataFrame({"widget_id": [1]}).write_csv(location)
    source = CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        directory=str(tmp_path),
        pattern="*.csv",
        archive_directory=str(tmp_path / "archive"),
        location=str(location),
    )
    with pytest.raises(SchemaConformanceError, match="color"):
        CsvReader(source).read().collect()


def test_file_readers_require_a_resolved_location(tmp_path: Path) -> None:
    csv_source = CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        directory=str(tmp_path),
        pattern="*.csv",
        archive_directory=str(tmp_path / "archive"),
    )
    parquet_source = ParquetSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        directory=str(tmp_path),
        pattern="*.parquet",
        archive_directory=str(tmp_path / "archive"),
    )
    with pytest.raises(ValueError, match="file location"):
        CsvReader(csv_source).read()
    with pytest.raises(ValueError, match="file location"):
        ParquetReader(parquet_source).read()


def test_runner_runs_a_database_source(
    tmp_path: Path,
    widget_frame: pl.LazyFrame,
    warehouse: Path,
) -> None:
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
    PipelineRunner(source).run()
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.drop(
        "etl_row_hash",
        "etl_created_on",
        "etl_updated_on",
        "is_source_system_active",
    ).equals(widget_frame.collect().sort("widget_id"))

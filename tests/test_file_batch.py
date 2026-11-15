from pathlib import Path

import polars as pl
import pytest
from deltalake.exceptions import CommitFailedError

from src.pipeline.audit.auditor import StagingAuditor
from src.pipeline.publish.publisher import Publisher
from src.pipeline.runner import PipelineRunner
from src.processor import Processor
from src.sources.csv_source import CSVSourceConfig
from src.sources.registry import SourceRegistry
from tests.fixtures.widgets import WidgetSchema


def _csv_source(tmp_path: Path, pattern: str = "widget_*.csv") -> CSVSourceConfig:
    scan = tmp_path / "incoming"
    archive = tmp_path / "archive"
    scan.mkdir()
    archive.mkdir()
    return CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        directory=str(scan),
        pattern=pattern,
        archive_directory=str(archive),
    )


def _process(source: CSVSourceConfig) -> None:
    registry = SourceRegistry()
    registry.add_sources([source])
    Processor(registry).process()


def test_file_batch_loads_matching_files_in_parallel(tmp_path: Path, warehouse: Path) -> None:
    source = _csv_source(tmp_path)
    scan = Path(source.directory)
    archive = Path(source.archive_directory)
    pl.DataFrame({"widget_id": [1], "color": ["red"]}).write_csv(scan / "widget_a.csv")
    pl.DataFrame({"widget_id": [2], "color": ["blue"]}).write_csv(scan / "widget_b.csv")
    pl.DataFrame({"widget_id": [3], "color": ["gold"]}).write_csv(scan / "other.csv")
    _process(source)
    published = pl.read_delta(f"{warehouse}/widgets").sort("widget_id")
    assert published.drop(
        "etl_row_hash",
        "etl_created_on",
        "etl_updated_on",
        "is_source_system_active",
    ).equals(pl.DataFrame({"widget_id": [1, 2], "color": ["red", "blue"]}))
    assert not (scan / "widget_a.csv").exists()
    assert not (scan / "widget_b.csv").exists()
    assert (scan / "other.csv").exists()
    assert (archive / "widget_a.csv").exists()
    assert (archive / "widget_b.csv").exists()
    assert not (warehouse / "staging_widget_a").exists()
    assert not (warehouse / "staging_widget_b").exists()


def test_file_batch_uses_sanitized_staging_name(
    tmp_path: Path,
    warehouse: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _csv_source(tmp_path, pattern="orders*.csv")
    scan = Path(source.directory)
    pl.DataFrame({"widget_id": [1], "color": ["red"]}).write_csv(scan / "orders (1).csv")
    seen: list[str] = []
    original = PipelineRunner.write

    def record(self, frame, staging_name=None):
        seen.append(staging_name or "")
        return original(self, frame, staging_name=staging_name)

    monkeypatch.setattr(PipelineRunner, "write", record)
    _process(source)
    assert seen == ["orders_1"]
    assert not (warehouse / "staging_orders_1").exists()


def test_file_batch_keeps_scan_file_when_publish_fails(
    tmp_path: Path,
    warehouse: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _csv_source(tmp_path)
    scan = Path(source.directory)
    archive = Path(source.archive_directory)
    path = scan / "widget_a.csv"
    pl.DataFrame({"widget_id": [1], "color": ["red"]}).write_csv(path)

    def fail(_self, _staging_table_name: str) -> None:
        raise ValueError("publish failed")

    monkeypatch.setattr(Publisher, "publish", fail)
    monkeypatch.setattr("src.utils.time.sleep", lambda _seconds: None)
    with pytest.raises(ValueError, match="publish failed"):
        _process(source)
    assert path.exists()
    assert (archive / "widget_a.csv").exists()
    assert (warehouse / "widgets").is_dir()


def test_file_batch_retries_commit_failed_error(
    tmp_path: Path,
    warehouse: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _csv_source(tmp_path)
    scan = Path(source.directory)
    path = scan / "widget_a.csv"
    pl.DataFrame({"widget_id": [1], "color": ["red"]}).write_csv(path)
    attempts = {"count": 0}
    original = Publisher.publish

    def flaky(self, staging_table_name: str) -> None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise CommitFailedError("conflict")
        original(self, staging_table_name)

    monkeypatch.setattr(Publisher, "publish", flaky)
    _process(source)
    assert attempts["count"] == 2
    assert not path.exists()
    published = pl.read_delta(f"{warehouse}/widgets")
    assert published.height == 1


def test_file_batch_retries_audit_failures(
    tmp_path: Path,
    warehouse: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _csv_source(tmp_path)
    scan = Path(source.directory)
    path = scan / "widget_a.csv"
    pl.DataFrame({"widget_id": [1], "color": ["red"]}).write_csv(path)
    attempts = {"count": 0}
    original = StagingAuditor.audit

    def flaky(self, staging_table_name: str) -> None:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise CommitFailedError("conflict")
        original(self, staging_table_name)

    monkeypatch.setattr(StagingAuditor, "audit", flaky)
    _process(source)
    assert attempts["count"] == 2
    assert not path.exists()
    assert pl.read_delta(f"{warehouse}/widgets").height == 1


def test_file_batch_empty_match_is_a_noop(tmp_path: Path, warehouse: Path) -> None:
    source = _csv_source(tmp_path)
    _process(source)
    assert pl.read_delta(f"{warehouse}/widgets").is_empty()


def test_file_batch_supports_object_store_uris(tmp_path: Path, warehouse: Path) -> None:
    scan = tmp_path / "incoming"
    archive = tmp_path / "archive"
    scan.mkdir()
    pl.DataFrame({"widget_id": [1], "color": ["red"]}).write_csv(scan / "widget_a.csv")
    source = CSVSourceConfig(
        name="widgets",
        source_schema=WidgetSchema,
        grain=["widget_id"],
        directory=scan.as_uri(),
        pattern="widget_*.csv",
        archive_directory=archive.as_uri(),
    )
    _process(source)
    assert pl.read_delta(f"{warehouse}/widgets").height == 1
    assert not (scan / "widget_a.csv").exists()
    assert (archive / "widget_a.csv").exists()


def test_file_batch_raises_after_commit_retries_are_exhausted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    warehouse: Path,
) -> None:
    source = _csv_source(tmp_path)
    scan = Path(source.directory)
    path = scan / "widget_a.csv"
    pl.DataFrame({"widget_id": [1], "color": ["red"]}).write_csv(path)

    def always_fail(_self, _staging_table_name: str) -> None:
        raise CommitFailedError("conflict")

    monkeypatch.setattr(Publisher, "publish", always_fail)
    monkeypatch.setattr("src.utils.time.sleep", lambda _seconds: None)
    with pytest.raises(CommitFailedError, match="conflict"):
        _process(source)
    assert path.exists()
    assert (warehouse / "widgets").is_dir()

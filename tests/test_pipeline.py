import threading
import time
from unittest.mock import MagicMock

import pytest
from polars import LazyFrame

from src.exceptions import (
    NON_RETRYABLE_EXCEPTIONS,
    ProcessingError,
    SchemaConformanceError,
)
from src.pipeline.audit.auditor import StagingAuditor
from src.pipeline.read.base import BaseReader
from src.pipeline.read.factory import ReaderFactory
from src.pipeline.runner import PipelineRunner, PipelineRunResult
from src.processor import Processor
from src.sources.base import SourceConfig
from src.sources.master import MASTER_REGISTRY
from src.sources.registry import ALL_CONFIGS, SourceRegistry
from src.utils import retry
from tests.fixtures.widgets import AliasedWidgetSchema, WidgetSchema


def test_non_retryable_exceptions_includes_schema_conformance() -> None:
    assert SchemaConformanceError in NON_RETRYABLE_EXCEPTIONS


def test_schema_converts_to_polars() -> None:
    assert WidgetSchema.to_polars_schema()["widget_id"].is_integer()


def test_schema_source_rename_map_uses_field_aliases() -> None:
    assert AliasedWidgetSchema.source_rename_map() == {
        "Widget ID": "widget_id",
        "Purchase Date": "purchase_date",
        "color": "color",
    }


def test_registry_collects_sources(widget_source: SourceConfig) -> None:
    registry = SourceRegistry()
    registry.add_sources([widget_source])
    assert registry.get_sources() == [widget_source]
    first = widget_source.model_copy(update={"name": "widget_a"})
    second = widget_source.model_copy(update={"name": "widget_b"})
    other = widget_source.model_copy(update={"name": "gadgets"})
    registry.add_sources([first, second, other])
    assert registry.find_sources("widget_*") == [first, second]
    assert registry.find_sources("widget_a") == [first]
    assert registry.find_sources("Widget_*") == []
    assert isinstance(MASTER_REGISTRY, SourceRegistry)
    assert isinstance(ALL_CONFIGS, SourceRegistry)


def test_processor_runs_each_source(
    widget_source: SourceConfig,
    monkeypatch: pytest.MonkeyPatch,
    warehouse,
) -> None:
    other = widget_source.model_copy(update={"name": "gadgets"})
    registry = SourceRegistry()
    registry.add_sources([widget_source, other])
    started: list[SourceConfig] = []

    def record(runner: PipelineRunner) -> PipelineRunResult:
        started.append(runner.source_config)
        return PipelineRunResult(success=True, source_name=runner.source_config.name)

    monkeypatch.setattr(PipelineRunner, "run", record)
    Processor(registry).process()
    assert {source.name for source in started} == {"widgets", "gadgets"}
    started.clear()
    Processor(registry).process("widgets")
    assert started == [widget_source]
    with pytest.raises(ValueError, match="missing"):
        Processor(registry).process("missing")
    assert (warehouse / "widgets").is_dir()
    assert (warehouse / "gadgets").is_dir()
    started.clear()
    matched = widget_source.model_copy(update={"name": "widget_extra"})
    registry.add_sources([matched])
    Processor(registry).process("widget_*")
    assert started == [matched]


def test_processor_runs_sources_in_parallel(
    widget_source: SourceConfig,
    monkeypatch: pytest.MonkeyPatch,
    warehouse,
) -> None:
    other = widget_source.model_copy(update={"name": "gadgets"})
    registry = SourceRegistry()
    registry.add_sources([widget_source, other])
    barrier = threading.Barrier(2, timeout=2)

    def record(runner: PipelineRunner) -> PipelineRunResult:
        assert runner.source_config.name in {"widgets", "gadgets"}
        barrier.wait()
        return PipelineRunResult(success=True, source_name=runner.source_config.name)

    monkeypatch.setattr(PipelineRunner, "run", record)
    Processor(registry).process()
    assert (warehouse / "widgets").is_dir()
    assert (warehouse / "gadgets").is_dir()


def test_runner_run_returns_success(
    widget_source,
    monkeypatch: pytest.MonkeyPatch,
    warehouse,
) -> None:
    monkeypatch.setattr(
        PipelineRunner,
        "read",
        lambda _self: LazyFrame({"widget_id": [1], "color": ["red"]}),
    )
    result = PipelineRunner(widget_source).run()
    assert result.success is True
    assert result.error is None
    assert (warehouse / "widgets").is_dir()


def test_runner_run_returns_failure_with_exception(
    widget_source,
    monkeypatch: pytest.MonkeyPatch,
    warehouse,
) -> None:
    def fail(_self, _staging_table_name: str) -> None:
        raise ValueError("audit failed")

    monkeypatch.setattr(
        PipelineRunner,
        "read",
        lambda _self: LazyFrame({"widget_id": [1], "color": ["red"]}),
    )
    monkeypatch.setattr(StagingAuditor, "audit", fail)
    monkeypatch.setattr("src.utils.time.sleep", lambda _seconds: None)
    result = PipelineRunner(widget_source).run()
    assert result.success is False
    assert isinstance(result.error, ValueError)
    assert str(result.error) == "audit failed"
    assert (warehouse / "widgets").is_dir()


def test_processor_raises_after_all_tasks_finish(
    widget_source: SourceConfig,
    monkeypatch: pytest.MonkeyPatch,
    warehouse,
) -> None:
    other = widget_source.model_copy(update={"name": "gadgets"})
    registry = SourceRegistry()
    registry.add_sources([widget_source, other])
    barrier = threading.Barrier(2, timeout=2)
    finished: list[str] = []

    def record(runner: PipelineRunner) -> PipelineRunResult:
        name = runner.source_config.name
        barrier.wait()
        finished.append(name)
        result = PipelineRunResult(success=True, source_name=name)
        if name == "widgets":
            result = PipelineRunResult(
                success=False,
                source_name=name,
                error=RuntimeError("widgets failed"),
            )
        return result

    notify = MagicMock()
    monkeypatch.setattr(PipelineRunner, "run", record)
    monkeypatch.setattr("src.processor.SlackNotifier.notify", notify)
    with pytest.raises(ProcessingError, match="widgets failed") as raised:
        Processor(registry).process()
    assert set(finished) == {"widgets", "gadgets"}
    assert len(raised.value.failures) == 1
    notify.assert_called_once()
    assert (warehouse / "widgets").is_dir()
    assert (warehouse / "gadgets").is_dir()


def test_reader_factory_returns_registered_reader(widget_source: SourceConfig) -> None:
    class StubReader(BaseReader):
        def _read_source(self) -> LazyFrame:
            return LazyFrame({"widget_id": [1], "color": ["red"]})

    ReaderFactory.readers["inline"] = StubReader
    try:
        reader = ReaderFactory.get_reader(widget_source)
        assert isinstance(reader.read(), LazyFrame)
    finally:
        del ReaderFactory.readers["inline"]


def test_reader_factory_rejects_unknown_source(widget_source: SourceConfig) -> None:
    with pytest.raises(ValueError, match="not found"):
        ReaderFactory.get_reader(widget_source)


def test_retry_skips_schema_conformance_error() -> None:
    calls = {"count": 0}

    @retry(attempts=3, delay=0, backoff=1)
    def broken() -> None:
        calls["count"] += 1
        raise SchemaConformanceError("color")

    with pytest.raises(SchemaConformanceError, match="color"):
        broken()
    assert calls["count"] == 1


def test_retry_returns_after_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda _wait: None)
    calls = {"count": 0}

    @retry(attempts=2, delay=0, backoff=1)
    def flaky() -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("nope")
        return "ok"

    assert flaky() == "ok"


def test_retry_raises_last_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda _wait: None)

    @retry(attempts=1, delay=0)
    def flaky() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError, match="nope"):
        flaky()


def test_retry_rejects_zero_attempts() -> None:
    @retry(attempts=0)
    def never() -> str:
        return "no"

    with pytest.raises(ValueError, match="Attempts"):
        never()

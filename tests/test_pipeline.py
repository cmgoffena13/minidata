import time

import pytest
from polars import LazyFrame

from src.pipeline.read.base import BaseReader
from src.pipeline.read.factory import ReaderFactory
from src.pipeline.runner import PipelineRunner
from src.processor import Processor
from src.sources.base import SourceConfig
from src.sources.master import MASTER_REGISTRY
from src.sources.registry import ALL_CONFIGS, SourceRegistry
from src.utils import retry
from tests.fixtures.widgets import WidgetSchema


def test_schema_converts_to_polars() -> None:
    assert WidgetSchema.to_polars_schema()["widget_id"].is_integer()


def test_registry_collects_sources(widget_source: SourceConfig) -> None:
    registry = SourceRegistry()
    registry.add_sources([widget_source])
    assert registry.get_sources() == [widget_source]
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

    def record(runner: PipelineRunner) -> None:
        started.append(runner.source_config)

    monkeypatch.setattr(PipelineRunner, "run", record)
    Processor(registry).process()
    assert started == [widget_source, other]
    assert (warehouse / "widgets").is_dir()
    assert (warehouse / "gadgets").is_dir()


def test_reader_factory_returns_registered_reader(widget_source: SourceConfig) -> None:
    class StubReader(BaseReader):
        def read(self) -> LazyFrame:
            super().read()
            return LazyFrame()

    ReaderFactory.readers["inline"] = StubReader
    try:
        reader = ReaderFactory.get_reader(widget_source)
        assert isinstance(reader.read(), LazyFrame)
    finally:
        del ReaderFactory.readers["inline"]


def test_reader_factory_rejects_unknown_source(widget_source: SourceConfig) -> None:
    with pytest.raises(ValueError, match="not found"):
        ReaderFactory.get_reader(widget_source)


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

from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from src.cli.main import app
from src.processor import Processor
from src.sources.master import MASTER_REGISTRY
from src.sources.registry import SourceRegistry
from tests.fixtures.widgets import WIDGET_SOURCE


def test_process_command_runs_master_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    registries: list[SourceRegistry] = []

    def record(self: Processor, source_name: str | None = None) -> None:
        registries.append(self.master_registry)
        assert source_name is None

    monkeypatch.setattr(Processor, "process", record)
    result = CliRunner().invoke(app, ["process"])
    assert result.exit_code == 0
    assert registries == [MASTER_REGISTRY]


def test_process_command_runs_named_source(monkeypatch: pytest.MonkeyPatch) -> None:
    names: list[str | None] = []

    def record(self: Processor, source_name: str | None = None) -> None:
        assert self.master_registry is MASTER_REGISTRY
        names.append(source_name)

    monkeypatch.setattr(Processor, "process", record)
    result = CliRunner().invoke(app, ["process", "widgets"])
    assert result.exit_code == 0
    assert names == ["widgets"]


def test_process_command_rejects_unknown_source() -> None:
    result = CliRunner().invoke(app, ["process", "widgets"])
    message = result.output + (result.stderr or "")
    assert result.exit_code == 1
    assert 'Source "widgets" not found' in message
    assert "Processing started" not in message
    assert "Traceback" not in message


def test_help_hides_completion_options() -> None:
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "process" in result.output
    assert "source" in result.output
    assert "--install-completion" not in result.output
    assert "--show-completion" not in result.output
    assert "╭" not in result.output


def test_source_lists_names_alphabetically(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = SourceRegistry()
    registry.add_sources(
        [
            WIDGET_SOURCE.model_copy(update={"name": "zeta"}),
            WIDGET_SOURCE.model_copy(update={"name": "alpha"}),
        ]
    )
    monkeypatch.setattr("src.cli.main.MASTER_REGISTRY", registry)
    result = CliRunner().invoke(app, ["source"])
    assert result.exit_code == 0
    assert result.output == "alpha\nzeta\n"


def test_source_filters_by_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    registry = SourceRegistry()
    registry.add_sources(
        [
            WIDGET_SOURCE.model_copy(update={"name": "widgets"}),
            WIDGET_SOURCE.model_copy(update={"name": "gadgets"}),
            WIDGET_SOURCE.model_copy(update={"name": "widget_extra"}),
        ]
    )
    monkeypatch.setattr("src.cli.main.MASTER_REGISTRY", registry)
    result = CliRunner().invoke(app, ["source", "widget*"])
    assert result.exit_code == 0
    assert result.output == "widget_extra\nwidgets\n"


def test_source_reports_no_matches_for_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.cli.main.MASTER_REGISTRY", SourceRegistry())
    result = CliRunner().invoke(app, ["source", "missing"])
    assert result.exit_code == 0
    assert result.output == 'No matches for "missing"\n'


def test_query_command_prints_dataframe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warehouse = tmp_path / "bronze"
    warehouse.mkdir()
    monkeypatch.setattr("src.settings.config.DATAWAREHOUSE_URL", str(warehouse))
    pl.DataFrame({"widget_id": [1]}).write_delta(warehouse / "widgets")
    result = CliRunner().invoke(app, ["query", "SELECT widget_id FROM bronze.widgets"])
    assert result.exit_code == 0
    assert "widget_id" in result.output
    assert "1" in result.output

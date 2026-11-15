import pytest
from typer.testing import CliRunner

from src.cli.main import app
from src.processor import Processor
from src.sources.master import MASTER_REGISTRY
from src.sources.registry import SourceRegistry


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
    assert "--install-completion" not in result.output
    assert "--show-completion" not in result.output
    assert "╭" not in result.output

import pytest
from typer.testing import CliRunner

from src.cli.main import app
from src.processor import Processor
from src.sources.master import MASTER_REGISTRY
from src.sources.registry import SourceRegistry


def test_process_command_runs_master_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    registries: list[SourceRegistry] = []

    def record(self: Processor) -> None:
        registries.append(self.master_registry)

    monkeypatch.setattr(Processor, "process", record)
    result = CliRunner().invoke(app, ["process"])
    assert result.exit_code == 0
    assert registries == [MASTER_REGISTRY]


def test_help_hides_completion_options() -> None:
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "process" in result.output
    assert "--install-completion" not in result.output
    assert "--show-completion" not in result.output
    assert "╭" not in result.output

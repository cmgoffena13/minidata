import pytest

from src.settings import DevConfig


def test_dev_config_uses_the_bronze_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEV_DATAWAREHOUSE_URL", raising=False)
    assert DevConfig(_env_file=None).DATAWAREHOUSE_URL == "datawarehouse/bronze"

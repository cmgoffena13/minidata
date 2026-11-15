import os
from pathlib import Path

# Needs to happen before local imports
os.environ["ENV_STATE"] = "test"

import polars as pl
import pytest
from polars import LazyFrame

from src.settings import config
from src.sources.base import SourceConfig
from tests.fixtures.widgets import WIDGET_ROWS, WIDGET_SOURCE


@pytest.fixture
def widget_source() -> SourceConfig:
    return WIDGET_SOURCE.model_copy(deep=True)


@pytest.fixture
def widget_frame() -> LazyFrame:
    return pl.LazyFrame(WIDGET_ROWS)


@pytest.fixture
def warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", str(tmp_path))
    return tmp_path

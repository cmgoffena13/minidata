from pathlib import Path

import polars as pl
import pytest

from src.query import query
from src.settings import config


def test_query_reads_delta_table_as_schema_dot_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warehouse = tmp_path / "bronze"
    warehouse.mkdir()
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", str(warehouse))
    pl.DataFrame({"widget_id": [1, 2], "color": ["red", "blue"]}).write_delta(warehouse / "widgets")
    result = query("SELECT widget_id, color FROM bronze.widgets ORDER BY widget_id")
    assert result.to_dicts() == [
        {"widget_id": 1, "color": "red"},
        {"widget_id": 2, "color": "blue"},
    ]


def test_query_requires_warehouse_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", None)
    with pytest.raises(ValueError, match="DATAWAREHOUSE_URL"):
        query("SELECT 1")


def test_query_rejects_remote_warehouse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "DATAWAREHOUSE_URL", "s3://bucket/bronze")
    with pytest.raises(ValueError, match="local warehouse"):
        query("SELECT 1")

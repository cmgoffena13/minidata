import polars as pl
import pytest
from xxhash import xxh128_hexdigest

from src.exceptions import SchemaConformanceError
from src.pipeline.polars_utils import add_etl_row_hash, conform_to_schema
from tests.fixtures.widgets import AliasedWidgetSchema, WidgetSchema


def test_add_etl_row_hash_uses_xxh128() -> None:
    frame = pl.LazyFrame({"widget_id": [1], "color": ["red"]})
    hashed = add_etl_row_hash(frame).collect()
    assert hashed.columns == ["widget_id", "color", "etl_row_hash"]
    assert hashed["etl_row_hash"].item() == xxh128_hexdigest(b"1|red")


def test_add_etl_row_hash_blanks_nulls() -> None:
    frame = pl.LazyFrame(
        {"widget_id": [1, None], "color": [None, "blue"]},
        schema={"widget_id": pl.Int64, "color": pl.String},
    )
    hashed = add_etl_row_hash(frame).collect()
    by_color = dict(zip(hashed["color"].to_list(), hashed["etl_row_hash"].to_list(), strict=True))
    assert by_color[None] == xxh128_hexdigest(b"1|")
    assert by_color["blue"] == xxh128_hexdigest(b"|blue")


def test_conform_to_schema_requires_schema_columns() -> None:
    frame = pl.LazyFrame({"widget_id": [1]})
    with pytest.raises(SchemaConformanceError, match="color"):
        conform_to_schema(
            frame,
            WidgetSchema.to_polars_schema(),
            WidgetSchema.source_rename_map(),
        ).collect()


def test_conform_to_schema_casts_and_keeps_extra_columns() -> None:
    frame = pl.LazyFrame({"widget_id": ["1"], "color": ["red"], "size": ["L"]})
    conformed = conform_to_schema(
        frame,
        WidgetSchema.to_polars_schema(),
        WidgetSchema.source_rename_map(),
    ).collect()
    assert conformed.schema["widget_id"] == pl.Int64
    assert conformed.columns == ["widget_id", "color", "size"]
    assert conformed.to_dicts() == [{"widget_id": 1, "color": "red", "size": "L"}]


def test_conform_to_schema_renames_aliased_source_columns() -> None:
    frame = pl.LazyFrame({"Widget ID": ["1"], "Purchase Date": ["2024-01-02"], "color": ["red"]})
    conformed = conform_to_schema(
        frame,
        AliasedWidgetSchema.to_polars_schema(),
        AliasedWidgetSchema.source_rename_map(),
    ).collect()
    assert conformed.columns == ["widget_id", "purchase_date", "color"]
    assert conformed.to_dicts() == [{"widget_id": 1, "purchase_date": "2024-01-02", "color": "red"}]


def test_conform_to_schema_accepts_field_names_when_alias_is_set() -> None:
    frame = pl.LazyFrame({"widget_id": ["1"], "purchase_date": ["2024-01-02"], "color": ["red"]})
    conformed = conform_to_schema(
        frame,
        AliasedWidgetSchema.to_polars_schema(),
        AliasedWidgetSchema.source_rename_map(),
    ).collect()
    assert conformed.to_dicts() == [{"widget_id": 1, "purchase_date": "2024-01-02", "color": "red"}]


def test_add_etl_row_hash_ignores_an_existing_hash() -> None:
    frame = pl.LazyFrame({"widget_id": [1], "color": ["red"]})
    once = add_etl_row_hash(frame).collect()
    twice = add_etl_row_hash(once.lazy()).collect()
    assert once.equals(twice)

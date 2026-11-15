import polars as pl
from xxhash import xxh128_hexdigest

from src.pipeline.polars_utils import add_etl_row_hash


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


def test_add_etl_row_hash_ignores_an_existing_hash() -> None:
    frame = pl.LazyFrame({"widget_id": [1], "color": ["red"]})
    once = add_etl_row_hash(frame).collect()
    twice = add_etl_row_hash(once.lazy()).collect()
    assert once.equals(twice)

from polars import LazyFrame, Series, String, col, concat_str
from xxhash import xxh128_hexdigest


def add_etl_row_hash(frame: LazyFrame) -> LazyFrame:
    columns = [name for name in frame.collect_schema().names() if name != "etl_row_hash"]
    payload = concat_str(
        [col(name).cast(String).fill_null("") for name in columns],
        separator="|",
    )
    return frame.with_columns(
        payload.map_batches(_hash_text_batch, return_dtype=String).alias("etl_row_hash")
    )


def _hash_text_batch(batch: Series) -> Series:
    return Series([xxh128_hexdigest(value.encode()) for value in batch.to_list()])

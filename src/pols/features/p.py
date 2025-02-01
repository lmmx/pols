from __future__ import annotations
import polars as pl


def append_slash(files: pl.DataFrame) -> pl.DataFrame:
    return files.with_columns(
        pl.when(pl.col("is_dir"))
        .then(pl.col("name") + "/")
        .otherwise(pl.col("name"))
        .alias("name")
    )

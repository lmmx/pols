from __future__ import annotations

import polars as pl


def make_size_human_readable(files: pl.DataFrame) -> pl.DataFrame:
    size_col = pl.col("size")
    return files.with_columns(
        size=pl.when(size_col < 1024)
        .then(pl.concat_str(size_col.cast(pl.String), pl.lit("")))
        .when(size_col < 1024 * 1024)
        .then(
            pl.concat_str(size_col.truediv(1024).round(1).cast(pl.String), pl.lit("K"))
        )
        .when(size_col < 1024 * 1024 * 1024)
        .then(
            pl.concat_str(
                size_col.truediv(1024 * 1024).round(0).cast(pl.String), pl.lit("M")
            )
        )
        .otherwise(
            pl.concat_str(
                size_col.truediv(1024 * 1024 * 1024).round(0).cast(pl.String),
                pl.lit("G"),
            )
        )
    )

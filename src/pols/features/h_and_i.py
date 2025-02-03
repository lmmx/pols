from __future__ import annotations

import polars as pl


def scale_unit_size(files: pl.DataFrame, base: int) -> pl.DataFrame:
    size_col = pl.col("size")
    return files.with_columns(
        size=pl.when(size_col < base)
        .then(pl.concat_str(size_col.cast(pl.String), pl.lit("")))
        .when(size_col < base * base)
        .then(
            pl.concat_str(size_col.truediv(base).round(1).cast(pl.String), pl.lit("K"))
        )
        .when(size_col < base * base * base)
        .then(
            pl.concat_str(
                size_col.truediv(base * base).round(0).cast(pl.String), pl.lit("M")
            )
        )
        .otherwise(
            pl.concat_str(
                size_col.truediv(base * base * base).round(0).cast(pl.String),
                pl.lit("G"),
            )
        )
    )


def make_size_si_unit(files: pl.DataFrame) -> pl.DataFrame:
    return scale_unit_size(files=files, base=1000)


def make_size_human_readable(files: pl.DataFrame) -> pl.DataFrame:
    return scale_unit_size(files=files, base=1024)

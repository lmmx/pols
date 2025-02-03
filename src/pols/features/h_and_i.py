from __future__ import annotations

import polars as pl

__all__ = (
    "format_size",
    "scale_unit_size",
    "make_size_si_unit",
    "make_size_human_readable",
)


def format_size(size_expr, unit):
    return pl.concat_str(
        pl.when(size_expr < 10)
        .then(size_expr.round(1).cast(pl.String))
        .otherwise(
            size_expr.ceil()
            .cast(pl.Int64)
            .cast(pl.String)
            .str.replace(".0", "", literal=True)
        ),
        pl.lit(unit),
    )


def scale_unit_size(files: pl.DataFrame, base: int) -> pl.DataFrame:
    size_col = pl.col("size")
    kb = size_col.truediv(base)
    mb = size_col.truediv(base * base)
    gb = size_col.truediv(base * base * base)

    return files.with_columns(
        size=pl.when(size_col < base)
        .then(size_col.cast(pl.String))
        .when(size_col < base * base)
        .then(format_size(kb, "K"))
        .when(size_col < base * base * base)
        .then(format_size(mb, "M"))
        .otherwise(format_size(gb, "G"))
    )


def make_size_si_unit(files: pl.DataFrame) -> pl.DataFrame:
    return scale_unit_size(files=files, base=1000)


def make_size_human_readable(files: pl.DataFrame) -> pl.DataFrame:
    return scale_unit_size(files=files, base=1024)

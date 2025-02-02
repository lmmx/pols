from __future__ import annotations

import polars as pl


def numeric_sort(name: pl.Expr) -> pl.Expr:
    return (
        pl.col("name")
        .str.extract_all(r"(\D+|\d+)")
        .alias("parts")
        .list.eval(
            pl.when(
                pl.element().str.to_integer(strict=False).is_null(),
            )
            .then(
                pl.element()
                .str.split("")
                .list.eval(
                    pl.element()
                    .cast(pl.Binary)
                    .bin.encode("hex")
                    .str.to_integer(base=16)
                )
            )
            .otherwise(
                pl.element().cast(pl.Binary).bin.encode("hex").cast(pl.List(pl.Int64)),
            )
        )
        .list.eval(pl.element().explode())
        .alias("binhex")
    )

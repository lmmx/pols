from __future__ import annotations

import polars as pl

__all__ = ("add_path_metadata", "sort_pipe")


def add_path_metadata(files: pl.LazyFrame) -> pl.LazyFrame:
    pth = pl.col("path")
    return files.with_columns(
        is_dir=pth.map_elements(lambda p: p.is_dir(), return_dtype=pl.Boolean),
        is_symlink=pth.map_elements(lambda p: p.is_symlink(), return_dtype=pl.Boolean),
    )


def sort_pipe(df: pl.LazyFrame, by: str | pl.Expr, descending: bool):
    return df.sort(by=by, maintain_order=True, descending=descending)

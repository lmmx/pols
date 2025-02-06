from __future__ import annotations

import polars as pl

__all__ = ("add_size_metadata", "add_size_metadata_deref_symlinks")


def add_size_metadata(files: pl.LazyFrame) -> pl.LazyFrame:
    size_column = [p.lstat().st_size for p in files.get_column("path")]
    return files.with_columns(
        size=pl.col("path").map_elements(
            lambda p: p.lstat().st_size, return_dtype=pl.Int64
        )
    )


def add_size_metadata_deref_symlinks(files: pl.LazyFrame) -> pl.LazyFrame:
    return files.with_columns(
        size=pl.col("path").map_elements(
            lambda p: p.stat().st_size, return_dtype=pl.Int64
        )
    )

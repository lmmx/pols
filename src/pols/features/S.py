from __future__ import annotations

import polars as pl

__all__ = ("add_size_metadata", "add_size_metadata_deref_symlinks")


def add_size_metadata(files: pl.DataFrame) -> pl.DataFrame:
    size_column = [p.lstat().st_size for p in files.get_column("path")]
    return files.with_columns(size=pl.Series(size_column))


def add_size_metadata_deref_symlinks(files: pl.DataFrame) -> pl.DataFrame:
    size_column = [p.stat().st_size for p in files.get_column("path")]
    return files.with_columns(size=pl.Series(size_column))

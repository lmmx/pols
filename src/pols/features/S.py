from __future__ import annotations

import polars as pl


def add_size_column(files: pl.DataFrame) -> pl.DataFrame:
    size_column = [p.stat().st_size for p in files.get_column("path")]
    return files.with_columns(size=pl.Series(size_column))

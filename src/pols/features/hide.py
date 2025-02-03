from __future__ import annotations

import polars as pl

__all__ = ("filter_out_pattern",)


def filter_out_pattern(files: pl.DataFrame, pattern: str) -> pl.DataFrame:
    """Pattern will be a non-empty regex string."""
    return files.filter(~pl.col("name").str.contains(pattern))

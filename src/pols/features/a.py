from __future__ import annotations

import polars as pl


def remove_hidden_files(files: pl.DataFrame) -> pl.DataFrame:
    return files.filter(~pl.col("name").str.starts_with("."))

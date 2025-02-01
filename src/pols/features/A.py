from __future__ import annotations

import polars as pl


def remove_hidden_rel_dirs(files: pl.DataFrame) -> pl.DataFrame:
    return files.filter(~pl.col("name").is_in([".", ".."]))

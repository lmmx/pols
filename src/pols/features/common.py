from __future__ import annotations

import polars as pl

__all__ = ("add_path_metadata",)


def add_path_metadata(files: pl.DataFrame) -> pl.DataFrame:
    pth = pl.col("path")
    return files.with_columns(
        is_dir=pth.map_elements(lambda p: p.is_dir(), return_dtype=pl.Boolean),
        is_symlink=pth.map_elements(lambda p: p.is_symlink(), return_dtype=pl.Boolean),
    )

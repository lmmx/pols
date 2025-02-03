from __future__ import annotations

import polars as pl

__all__ = (
    "add_time_metadata",
    "add_time_metadata_deref_symlinks",
    "add_permissions_metadata",
    "add_permissions_metadata_deref_symlinks",
    "add_owner_group_metadata",
    "add_owner_group_metadata_deref_symlinks",
    "add_symlink_targets",
)


def add_time_metadata(
    files: pl.DataFrame, time_metric: Literal["atime", "ctime", "mtime"]
) -> pl.DataFrame:
    time_stat = f"st_{time_metric}"
    return files.with_columns(
        time=pl.col("path")
        .map_elements(lambda p: getattr(p.lstat(), time_stat), return_dtype=pl.Float64)
        .mul(1000)
        .cast(pl.Datetime("ms")),
    )


def add_time_metadata_deref_symlinks(
    files: pl.DataFrame,
    time_metric: Literal["atime", "ctime", "mtime"],
) -> pl.DataFrame:
    time_stat = f"st_{time_metric}"
    return files.with_columns(
        time=pl.col("path")
        .map_elements(lambda p: getattr(p.stat(), time_stat), return_dtype=pl.Float64)
        .mul(1000)
        .cast(pl.Datetime("ms")),
    )


def add_permissions_metadata(files: pl.DataFrame) -> pl.DataFrame:
    def get_mode_string(path):
        mode = path.lstat().st_mode
        return stat.filemode(mode)

    return files.with_columns(
        permissions=pl.col("path").map_elements(get_mode_string, return_dtype=pl.Utf8)
    )


def add_permissions_metadata_deref_symlinks(files: pl.DataFrame) -> pl.DataFrame:
    def get_mode_string(path):
        mode = path.stat().st_mode
        return stat.filemode(mode)

    return files.with_columns(
        permissions=pl.col("path").map_elements(get_mode_string, return_dtype=pl.Utf8)
    )


def add_owner_group_metadata(files: pl.DataFrame) -> pl.DataFrame:
    return files.with_columns(
        owner=pl.col("path").map_elements(
            lambda p: pwd.getpwuid(p.lstat().st_uid).pw_name, return_dtype=pl.Utf8
        ),
        group=pl.col("path").map_elements(
            lambda p: grp.getgrgid(p.lstat().st_gid).gr_name, return_dtype=pl.Utf8
        ),
    )


def add_owner_group_metadata_deref_symlinks(files: pl.DataFrame) -> pl.DataFrame:
    return files.with_columns(
        owner=pl.col("path").map_elements(
            lambda p: pwd.getpwuid(p.stat().st_uid).pw_name, return_dtype=pl.Utf8
        ),
        group=pl.col("path").map_elements(
            lambda p: grp.getgrgid(p.stat().st_gid).gr_name, return_dtype=pl.Utf8
        ),
    )


def add_symlink_targets(files: pl.DataFrame) -> pl.DataFrame:
    symlink_targets = [
        p.readlink() if is_link else None
        for p, is_link in zip(files.get_column("path"), files.get_column("is_symlink"))
    ]
    return files.with_columns(
        symlink_target=pl.Series(symlink_targets, dtype=pl.Object)
    )

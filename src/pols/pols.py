from __future__ import annotations

from functools import partial, reduce
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Literal, TypeAlias

import polars as pl

from .features.hide import filter_out_pattern
from .features.p import append_slash

if TYPE_CHECKING:
    import polars as pl

TimeFormat: TypeAlias = str


def pols(
    *paths: tuple[str | Path],
    a: bool = False,
    A: bool = False,
    author: bool = False,
    c: bool = False,
    d: bool = False,
    full_time: bool = False,
    group_directories_first: bool = False,
    G: bool = False,
    h: bool = False,
    si: bool = False,
    H: bool = False,
    dereference_command_line_symlink_to_dir: bool = False,
    hide: str | None = None,
    i: bool = False,
    I: str | None = None,
    l: bool = False,
    L: bool = False,
    p: bool = False,
    r: bool = False,
    R: bool = False,
    S: bool = False,
    sort: Literal["size", "time", "version", "extension"] | None = None,
    time: (
        Literal["atime", "access", "use", "ctime", "status", "birth", "creation"] | None
    ) = None,
    time_style: (
        Literal["full-iso", "long-iso", "iso", "locale"] | TimeFormat
    ) = "locale",
    u: bool = False,
    U: bool = False,
    v: bool = False,
    X: bool = False,
    t: bool = False,
    # Rest are additions to the ls flags
    keep_path: bool = False,
    keep_fs_metadata: bool = False,
) -> pl.DataFrame:
    """
    List the contents of a directory as Polars DataFrame.

    Args:
      [x] a: Do not ignore entries starting with `.`.
      [x] A: Do not list implied `.` and `..`.
      [ ] author: With `l`, print the author of each file.
      [ ] c: With `l` and `t` sort by, and show, ctime (time of last modification of file
         status information);
         with `l`: show ctime and  sort  by name;
         otherwise: sort by ctime, newest first.
      [ ] d: List directories themselves, not their contents.
      [ ] full_time: Like `l` with `time_style=full-iso`.
      [ ] group_directories_first: Group directories before files; can be augmented with a
                               `sort` option, but any use of `sort=None` (`U`)
                               disables grouping.
      [ ] G: In a long listing, don't print group names.
      [ ] h: With `l` and `s`, print sizes like 1K 234M 2G etc.
      [ ] si: Like `h`, but use powers of 1000 not 1024.
      [ ] H: Follow symbolic links listed on the command line.
      [ ] dereference_command_line_symlink_to_dir: Follow each command line symbolic link
                                               that points to a directory.
      [x] hide: Do not list implied entries matching shell pattern.
      [ ] i: Print the index number of each file.
      [x] I: Do not list implied entries matching shell pattern. Short code for `hide`.
      [ ] l: Use a long listing format.
      [ ] L: When showing file information for a symbolic link, show information for the
         file the link references rather than for the link itself.
      [x] p: Append `/` indicator to directories.
      [ ] r: Reverse order while sorting.
      [ ] R: List directories recursively.
      [ ] S: Sort by file size, largest first.
      [ ] sort: sort by WORD instead of name: None (`U`), size (`S`), time (`t`), version
            (`v`), extension (`X`).
      [ ] time: change  the default of using modification times:
              - access time (`u`): atime, access, use
              - change time (`c`): ctime, status
              - birth time:  birth, creation
            with  `l`,  value determines which time to show; with `sort=time`, sort by
            chosen time type (newest first).
      time_style: time/date format with `l`; argument can be full-iso, long-iso, iso,
                  locale, or +FORMAT. FORMAT is interpreted like in `datetime.strftime`.
      [ ] u: with `l` and `t`: sort by, and show, access time; with `l`: show access time
         and sort by name; otherwise: sort by access time, newest first.
      [ ] U: Do not sort; list entries in directory order.
      [ ] v: Natural sort of (version) numbers within text, i.e. numeric, non-lexicographic
         (so "file2" comes after "file10" etc.).
      [ ] X: Sort alphabetically by entry extension.
      [ ] t: Sort by time, newest first
      [x] keep_path: Keep a path column with the Pathlib path object.
      [x] keep_fs_metadata: Keep filesystem metadata booleans: `is_dir`, `is_symlink`.

        >>> pls()
        shape: (77, 2)
        ┌───────────────┬─────────────────────┐
        │ name          ┆ mtime               │
        │ ---           ┆ ---                 │
        │ str           ┆ datetime[ms]        │
        ╞═══════════════╪═════════════════════╡
        │ my_file.txt   ┆ 2025-01-31 13:10:27 │
        │ …             ┆ …                   │
        │ another.txt   ┆ 2025-01-31 13:44:43 │
        └───────────────┴─────────────────────┘
    """
    # Handle short codes
    hide = hide or I
    hidden_files_allowed = A or a

    drop_cols = [
        *([] if keep_path else ["path"]),
        *([] if keep_fs_metadata else ["is_dir", "is_symlink"]),
        "rel_to",
    ]

    # Identify the files to operate on
    individual_files = []
    dirs_to_scan = []
    nonexistent = []
    unexpanded_paths = list(map(Path, paths or (".",)))
    expanded_paths = []

    for path in unexpanded_paths:
        # Expand kleene star
        if any("*" in p for p in path.parts):
            # Remove double kleene stars, we don't support recursive **
            if any("**" in p for p in path.parts):
                path = Path(*[re.sub(r"\*+", "*", part) for part in p.parts])

            glob_base = Path(*[part for part in path.parts if "*" not in part])
            glob_subpath = path.relative_to(glob_base)
            expanded_paths.extend(*glob_base.glob(glob_subpath))
        else:
            expanded_paths.append(path)

    for path in expanded_paths:
        if path.is_file():
            individual_files.append(path)
        elif path.is_dir():
            dirs_to_scan.append(path)
        elif not path.exists():
            nonexistent.append(
                FileNotFoundError(
                    f"pols: cannot access 'path': No such file or directory"
                )
            )
    if nonexistent:
        raise FileNotFoundError(f"No such file:")

    pipes = [
        *([partial(filter_out_pattern, pattern=hide)] if hide else []),
        # Add symlink and directory bools from Path methods
        add_path_metadata,
        *([append_slash] if p else []),
    ]

    results = []
    for idx, path_set in enumerate((individual_files, *dirs_to_scan)):
        is_dir = idx > 0
        if not path_set:
            assert idx == 0  # This should only be when no files
            continue
        if is_dir:
            dir_root = path_set
            path_set = [
                *([Path("."), Path("..")] if a and not A else []),
                *(
                    f.relative_to(dir_root)
                    for f in path_set.iterdir()
                    if (hidden_files_allowed or not f.name.startswith("."))
                ),
            ]
        else:
            dir_root = Path()
            subpaths = path_set
        # e.g. `pols src` would give dir_root=src to `.`, `..`, and all in `.iterdir()`
        try:
            file_entries = []
            for path in path_set:
                entry = {
                    "path": path,
                    "name": str(
                        path
                        if path.is_absolute() or is_dir
                        else path.absolute().relative_to(dir_root.absolute())
                    ),
                    "rel_to": dir_root,
                }
                file_entries.append(entry)
            files = pl.DataFrame(file_entries)
        except Exception as e:
            print(f"Got no files from {path_set} due to {e}")
            raise
        path_set_result = reduce(pl.DataFrame.pipe, pipes, files).drop(drop_cols)
        # print(path_set_result.to_dicts())
        results.append(path_set_result)
    return results


def add_path_metadata(files):
    pth = pl.col("path")
    return files.with_columns(
        is_dir=pth.map_elements(lambda p: p.is_dir(), return_dtype=pl.Boolean),
        is_symlink=pth.map_elements(lambda p: p.is_symlink(), return_dtype=pl.Boolean),
    )

from __future__ import annotations

from functools import partial, reduce
from pathlib import Path
from sys import argv, stderr, stdout
from typing import TYPE_CHECKING, Callable, Literal, TypeAlias

import polars as pl

from .features.hide import filter_out_pattern
from .features.p import append_slash
from .features.v import numeric_sort

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
    sort: Literal[None, "size", "time", "version", "extension"] = None,
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
    print_to: TextIO | None = stdout,
    error_to: TextIO | None = stderr,
    to_dicts: bool = False,
    raise_on_access: bool = False,
    debug: bool = False,
) -> pl.DataFrame:
    """
    List the contents of a directory as Polars DataFrame.

    Args:
      [x] a: Do not ignore entries starting with `.`.
      [x] A: Do not list implied `.` and `..`.
      [ ] author: With `l`, print the author of each file.
      [x] c: With `l` and `t` sort by, and show, ctime (time of last modification of file
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
      [x] sort: sort by WORD instead of name: none (`U`), size (`S`), time (`t`), version
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
      [x] X: Sort alphabetically by entry extension.
      [x] t: Sort by time, newest first
      [x] keep_path: Keep a path column with the Pathlib path object.
      [x] keep_fs_metadata: Keep filesystem metadata booleans: `is_dir`, `is_symlink`.
      [x] print_to: Where to print to, by default writes to STDOUT, `None` to disable.
      [x] error_to: Where to error to, by default writes to STDERR, `None` to disable.
      [x] to_dicts: Return the result as dicts.
      [x] raise_on_access: Raise an error if a file cannot be accessed.

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
            glob_subpattern = str(path.relative_to(glob_base))
            expanded_paths.extend(list(glob_base.glob(glob_subpattern)))
        else:
            expanded_paths.append(path)

    for path in expanded_paths:
        try:
            is_file = path.is_file()
        except OSError as e:
            print(f"pols: cannot access '{path}': {e}", file=error_to)
            if raise_on_access:
                raise
            continue
        if is_file:
            individual_files.append(path)
        elif path.is_dir():
            dirs_to_scan.append(path)
        elif not path.exists():
            nonexistent.append(
                FileNotFoundError(
                    f"pols: cannot access '{path}': No such file or directory"
                )
            )
    if nonexistent:
        excs = ExceptionGroup(f"No such file:", nonexistent)
        if raise_on_access:
            raise excs
        else:
            print(excs, file=error_to)

    sort_pipes = []
    # none (`U`), size (`S`), time (`t`), version (`v`), extension (`X`).

    # Recreate the CLI order, N.B. will not be ordered from Python library call
    # (unsure if there's a workaround using inspect?)
    sortable = {"sort", "S", "t", "v", "c", "X"}
    # Take the flags and use their local values (i.e. parsed param values)
    sort_order = [k.lstrip("-") for k in argv if k.lstrip("-") in sortable]
    sort_sequence = {sort_key: locals()[sort_key] for sort_key in sort_order}
    sort_lookup = {
        "none": "U",
        "size": "S",
        "time": "t",
        "version": "v",
        "extension": "X",
    }
    # If a `--sort` was specified, set the corresponding value to True
    if "sort" in sort_sequence:
        sort_ref = sort_lookup[sort_sequence["sort"]]
        # Cannot simply set it to True as it would be last in the order
        ss_idx = (ss_lst := list(sort_sequence.items())).index("sort")
        # Overwrites the sort flag with the referenced flag with a value of True
        sort_sequence = dict([*ss_lst[:ss_idx, (sort_ref, True), ss_lst[ss_idx + 1 :]]])

    if sort_sequence:
        # Sort in reverse order of specification so sorts given first are applied last
        for sort_flag, sort_val in reversed(sort_sequence.items()):
            if sort_val is False:
                continue
            match sort_flag:
                case "U":
                    continue  # Do not sort
                case "S":
                    # This may cause a `MapWithoutReturnDtypeWarning` but it errors with
                    # `return_dtype` set as either int or pl.Int64 but works without!
                    # TODO: change this to a function
                    sort_by = pl.col("path").map_elements(lambda p: p.stat().st_size)
                # case "t":
                #     sort_by = pl.col("name").str.split(".").list.last()
                case "v":
                    sort_by = numeric_sort(pl.col("name"))
                case "X":
                    sort_by = pl.col("name").str.split(".").list.last()
                case _:
                    raise ValueError(f"Invalid flag in sort sequence {sort_flag}")

            sort_func = lambda df: df.sort(by=sort_by, maintain_order=True)
            sort_pipes.append(sort_func)
    else:
        lexico_sort = lambda df: df.sort(
            by=pl.col("name").str.to_lowercase(), maintain_order=True
        )
        sort_pipes.append(lexico_sort)

    pipes = [
        *([partial(filter_out_pattern, pattern=hide)] if hide else []),
        # Add symlink and directory bools from Path methods
        add_path_metadata,
        *([append_slash] if p else []),
        *([] if U else sort_pipes),
    ]

    results = []
    failures = []
    for idx, path_set in enumerate((individual_files, *dirs_to_scan)):
        is_dir = idx > 0
        if not path_set:
            assert idx == 0  # This should only be when no files
            continue
        if is_dir:
            dir_root = path_set
            path_set = [
                *([Path("."), Path("..")] if a and not A else []),
            ]
            for path_set_file in dir_root.iterdir():
                if hidden_files_allowed or not path_set_file.name.startswith("."):
                    try:
                        # Just do this to try to trigger an OSError to discard it early
                        path_set_file.is_file()
                    except OSError as e:
                        print(
                            f"pols: cannot access '{path_set_file}': {e}", file=error_to
                        )
                        if raise_on_access:
                            raise
                        continue
                    else:
                        path_set.append(path_set_file.relative_to(dir_root))
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
            failures.extend([ValueError(f"Got no files from {path_set} due to {e}"), e])
            if raise_on_access:
                raise e
            else:
                print(e, file=error_to)
            continue
        path_set_result = reduce(pl.DataFrame.pipe, pipes, files).drop(drop_cols)
        # print(path_set_result.to_dicts())
        results.append(
            {dir_root.name if not dir_root.name else str(dir_root): path_set_result}
        )
    if print_to:
        for result in results:
            [(source, paths)] = result.items()
            if source:
                print(f"{source}:", file=print_to)
            print(paths, file=print_to)
            if debug:
                breakpoint()
    return results if to_dicts else None


def add_path_metadata(files):
    pth = pl.col("path")
    return files.with_columns(
        is_dir=pth.map_elements(lambda p: p.is_dir(), return_dtype=pl.Boolean),
        is_symlink=pth.map_elements(lambda p: p.is_symlink(), return_dtype=pl.Boolean),
    )


# Known bugs: `pols *` in home dir fails, maybe permissions error?

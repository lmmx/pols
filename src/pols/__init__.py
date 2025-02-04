from __future__ import annotations

import os.path
import re
from functools import partial, reduce
from io import TextIOWrapper
from os import devnull
from pathlib import Path
from sys import argv, stderr, stdout
from typing import Literal, TypeAlias

import polars as pl

try:
    from line_profiler import profile
except ImportError:

    def profile(func):
        """No-op decorator if line_profiler is not available"""
        return func


from .features.common import add_path_metadata, sort_pipe
from .features.h_and_i import make_size_human_readable, make_size_si_unit
from .features.hide import filter_out_pattern
from .features.l import (
    add_owner_group_metadata,
    add_owner_group_metadata_deref_symlinks,
    add_permissions_metadata,
    add_permissions_metadata_deref_symlinks,
    add_symlink_targets,
    add_time_metadata,
    add_time_metadata_deref_symlinks,
)
from .features.p import append_slash
from .features.S import add_size_metadata, add_size_metadata_deref_symlinks
from .features.v import numeric_sort
from .resegment import resegment_raw_path
from .walk import flat_descendants

TimeFormat: TypeAlias = str


@profile
def ls(
    *paths: tuple[str | Path],
    a: bool = False,
    A: bool = False,
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
    I: str | None = None,
    l: bool = False,
    L: bool = False,
    p: bool = False,
    r: bool = False,
    R: bool = False,
    S: bool = False,
    sort: Literal[None, "size", "time", "version", "extension"] = None,
    time: Literal[
        "atime", "access", "use", "ctime", "status", "birth", "creation", "mtime"
    ] = "mtime",
    time_style: (
        Literal["full-iso", "long-iso", "iso", "locale"] | TimeFormat
    ) = "locale",
    u: bool = False,
    U: bool = False,
    v: bool = False,
    X: bool = False,
    t: bool = False,
    # Rest are additions to the ls flags
    as_path: bool = False,
    print_to: TextIOWrapper | Literal["stdout", "stderr", "devnull"] | None = None,
    error_to: TextIOWrapper | Literal["stderr", "stdout", "devnull"] | None = None,
    to_dict: bool = False,
    to_dicts: bool = False,
    raise_on_access: bool = False,
    debug: bool = False,
    inspect: bool = False,
    drop_only: str | None = None,
    keep: str | None = None,
    merge_all: bool = False,
    with_filter: str | None = None,
) -> pl.LazyFrame:
    """
    List the contents of a directory as Polars LazyFrame.

    Args:
      [x] a: Do not ignore entries starting with `.`.
      [x] A: Do not list implied `.` and `..`.
      [x] c: With `l` and `t` sort by, and show, ctime (time of last modification of file
         status information);
         with `l`: show ctime and sort by name;
         otherwise: sort by ctime, newest first.
      [x] d: List directories themselves, not their contents.
      [ ] full_time: Like `l` with `time_style=full-iso`.
      [ ] group_directories_first: Group directories before files; can be augmented with a
                               `sort` option, but any use of `sort=None` (`U`)
                               disables grouping.
      [x] G: In a long listing, don't print group names.
      [X] h: With `l` and `s`, print sizes like 1K 234M 2G etc.
      [x] si: Like `h`, but use powers of 1000 not 1024.
      [ ] H: Follow symbolic links listed on the command line.
      [ ] dereference_command_line_symlink_to_dir: Follow each command line symbolic link
                                               that points to a directory.
      [x] hide: Do not list implied entries matching shell pattern.
      [x] I: Do not list implied entries matching shell pattern. Short code for `hide`.
      [x] l: Use a long listing format.
      [ ] L: When showing file information for a symbolic link, show information for the
         file the link references rather than for the link itself.
      [x] p: Append `/` indicator to directories.
      [x] r: Reverse order while sorting.
      [x] R: List directories recursively.
      [x] S: Sort by file size, largest first.
      [x] sort: sort by WORD instead of name: none (`U`), size (`S`), time (`t`), version
            (`v`), extension (`X`).
      [x] time: change  the default of using modification times:
              - access time (`u`): atime, access, use
              - change time (`c`): ctime, status
              - birth time:  birth, creation
            with  `l`,  value determines which time to show; with `sort=time`, sort by
            chosen time type (newest first).
      time_style: time/date format with `l`; argument can be full-iso, long-iso, iso,
                  locale, or +FORMAT. FORMAT is interpreted like in `datetime.strftime`.
      [x] u: with `l` and `t`: sort by, and show, access time; with `l`: show access time
         and sort by name; otherwise: sort by access time, newest first.
      [x] U: Do not sort; list entries in directory order.
      [x] v: Natural sort of (version) numbers within text, i.e. numeric, non-lexicographic
         (so "file2" comes after "file10" etc.).
      [x] X: Sort alphabetically by entry extension.
      [x] t: Sort by time, newest first
      [x] as_path: Return the path column containing the Pathlib path object rather than
                   string name column.
      [x] print_to: Where to print to, by default writes to STDOUT, `None` to disable.
      [x] error_to: Where to error to, by default writes to STDERR, `None` to disable.
      [x] to_dict: Return the result as dict (key is the source: for individual files
          the source is the current path `"."`, for directory contents it is the parent
          directory).
      [x] to_dicts: Return the result as dicts.
      [x] raise_on_access: Raise an error if a file cannot be accessed.
      [x] inspect: Breakpoint on the final result after it is printed.
      [x] debug: Print verbose report output when path walking directory descendants.
                 Implies `inspect`, breakpoints on the final result after it is printed.
      [x] drop_only: Comma-separated string of column names to keep (default: None,
                         will not override standard list of columns to drop).
      [x] keep: Comma-separated string of column names to keep (default: None,
                         will not keep any of the standard list of columns to drop).
      [x] merge_all: Merge all results into a single LazyFrame with a column to preserve
                     their source directory (this is the empty string for individual files
                     or when there is only a single directory being listed).
      [x] with_filter: Either a column name (must be present in the LazyFrame or will
                       fail) or a Polars `Expr`, or a string that evaluates to one.
                       Implies `merge_all` (filtering unmerged LazyFrames risks some
                       source directory sources having 0 rows, raising an error when
                       concatenated).

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

    TOFIX:
    - `S` flag does not seem to work correctly, change to a function and unpack paths
      manually to create new column with values.
    """
    merge_all = with_filter is not None or merge_all  # with_filter implies merge_all
    inspect = debug or inspect
    if si and h:
        raise SystemExit(
            "Cannot set both `h` and `si` (conflicting bases for file size)"
        )
    printer_lookup = {
        "stdout": stdout,
        "stderr": stderr,
        "devnull": devnull,
    }

    if isinstance(print_to, str):
        print_to = printer_lookup.get(print_to)
    elif print_to is None:
        print_to = stdout

    if isinstance(error_to, str):
        error_to = printer_lookup.get(error_to)
    elif error_to is None:
        error_to = stderr

    if to_dict and to_dicts:
        raise ValueError("Please pass only one of `to_dict` and `to_dicts`.")
    # Handle short codes
    hide = hide or I
    hidden_files_allowed = A or a
    implied_time_sort = (c or u) and ((not l) or (t and l))
    time_lookup = {
        **{k: "atime" for k in "atime access use".split()},
        **{k: "ctime" for k in "ctime status birth creation".split()},
        "mtime": "mtime",
    }
    if u:
        lut_time = "atime"
    elif c:
        lut_time = "ctime"
    else:
        lut_time = time
    try:
        time_metric = time_lookup[lut_time]
    except KeyError as exc:
        raise ValueError(
            f"{time!r} is not a valid time: must be one of {[*time_lookup]}",
        ) from exc

    drop_cols_switched = [
        *(["name"] if as_path else ["path"]),
        *(["size"] if S and not l else []),
        *(["group"] if G else []),
        *(["time"] if not l and (t or implied_time_sort) else []),
        "is_symlink",
        "is_dir",
        "rel_to",
    ]
    drop_cols_kept = (
        drop_cols_switched
        if keep is None
        else [k for k in drop_cols_switched if k not in keep.split(",")]
    )
    drop_cols = (
        drop_cols_kept
        if drop_only is None
        else (drop_only.split(",") if drop_only else [])
    )

    # Identify the files to operate on
    individual_files = []
    dirs_to_scan = []
    nonexistent = []
    unexpanded_paths = list(map(Path, paths or (".",)))
    expanded_paths = []

    for path in unexpanded_paths:
        # Expand kleene star
        try:
            if any("*" in p for p in path.parts):
                # Remove double kleene stars, we don't support recursive **
                if any("**" in p for p in path.parts):
                    path = Path(*[re.sub(r"\*+", "*", part) for part in p.parts])

                glob_base = Path(*[part for part in path.parts if "*" not in part])
                # glob_subpattern = str(path.relative_to(glob_base))
                glob_subpattern = os.path.relpath(path, glob_base)
                globbed_paths = list(glob_base.glob(glob_subpattern))
                if not globbed_paths:
                    raise FileNotFoundError("No such file or directory")
                expanded_paths.extend(globbed_paths)
            else:
                expanded_paths.append(path)
        except OSError as e:
            # This includes FileNotFoundError we threw as well as access errors
            if error_to != devnull:
                print(f"pols: cannot access '{path}': {e}", file=error_to)
            if raise_on_access:
                raise
            continue

    for path in expanded_paths:
        try:
            if not path.exists():
                raise FileNotFoundError("No such file or directory")
            is_file = path.is_file()
        except OSError as e:
            # This includes FileNotFoundError we threw as well as access errors
            if error_to != devnull:
                print(f"pols: cannot access '{path}': {e}", file=error_to)
            if raise_on_access:
                raise
            continue
        if is_file:
            individual_files.append(path)
        elif path.is_dir():
            if d:
                individual_files.append(path)
            else:
                dirs_to_scan.append(path)
        elif not path.exists():
            nonexistent.append(
                FileNotFoundError(
                    f"pols: cannot access '{path}': No such file or directory"
                )
            )
    if nonexistent:
        excs = ExceptionGroup("No such file:", nonexistent)
        if raise_on_access:
            raise excs
        else:
            if error_to != devnull:
                print(excs, file=error_to)

    if R:
        for unscanned_dir in dirs_to_scan[:]:
            if unscanned_dir.is_absolute():
                # Simple case, can use `pathlib.Path.walk()`
                descendant_dirs = [dir_p for dir_p, _, _ in unscanned_dir.walk()][1:]
            else:
                # Construct from parts for the `walk_root_rel_raw_paths` function
                raw_usd = resegment_raw_path(unscanned_dir)
                # Must preserve raw paths carefully. Flatten sublevel lists first
                desc_dir_strs = flat_descendants(
                    raw_usd, hidden=hidden_files_allowed, report=debug
                )
                descendant_dirs = [resegment_raw_path(Path(dd)) for dd in desc_dir_strs]
            dirs_to_scan.extend(descendant_dirs)

    sort_pipes = []
    # none (`U`), size (`S`), time (`t`), version (`v`), extension (`X`).
    sort_lookup = {
        "none": "U",
        "size": "S",
        "time": "t",
        "version": "v",
        "extension": "X",
    }
    # Recreate the CLI order, N.B. will not be ordered from Python library call
    # (unsure if there's a workaround using inspect?)
    sortable = {"sort", "S", "t", "v", "X"}
    # We also need `c` and `u` (which imply `t` sort unless with `l`)
    if implied_time_sort:
        sortable = sortable.union({"c", "u"})
        sort_lookup.update({"c": "t", "u": "t"})
    # Take the flags and use their local values (i.e. parsed param values)
    sort_order = [k.lstrip("-") for k in argv if k.lstrip("-") in sortable]
    klocals = locals()
    sort_sequence = {sort_key: klocals[sort_key] for sort_key in sort_order}
    # If a `--sort` was specified, set the corresponding value to True
    if "sort" in sort_sequence:
        sort_ref = sort_lookup[sort_sequence["sort"]]
        # Cannot simply set it to True as it would be last in the order
        ss_idx = (ss_lst := list(sort_sequence.items())).index("sort")
        # Overwrites the sort flag with the referenced flag with a value of True
        sort_sequence = dict([*ss_lst[:ss_idx, (sort_ref, True), ss_lst[ss_idx + 1 :]]])

    if sort_sequence:
        # Sort in order of specification so sorts given first are applied first
        for sort_flag, sort_val in sort_sequence.items():
            sort_desc = False
            if sort_val is False:
                continue
            match sort_flag:
                case "U":
                    continue  # Do not sort
                case "S":
                    # This may cause a `MapWithoutReturnDtypeWarning` but it errors with
                    # `return_dtype` set as either int or pl.Int64 but works without!
                    sort_desc = True
                    sort_by = "size"
                    # Separate the size column computation from the sorting on it
                case "t" | "u" | "c":
                    sort_by = "time"
                    sort_desc = True
                case "v":
                    sort_by = numeric_sort(pl.col("name"))
                case "X":
                    sort_by = pl.col("name").str.split(".").list.last()
                case _:
                    raise ValueError(f"Invalid flag in sort sequence {sort_flag}")

            sort_pipes.append(partial(sort_pipe, by=sort_by, descending=sort_desc))
    else:
        lex_order = pl.col("name").str.to_lowercase()
        sort_pipes.append(partial(sort_pipe, by=lex_order, descending=False))
    if r and not U:
        sort_pipes.append(pl.LazyFrame.reverse)

    if L:
        permissions_pipe = add_permissions_metadata_deref_symlinks
        owner_group_pipe = add_owner_group_metadata_deref_symlinks
        size_pipe = add_size_metadata_deref_symlinks
        time_pipe_fd = add_time_metadata_deref_symlinks
    else:
        permissions_pipe = add_permissions_metadata
        owner_group_pipe = add_owner_group_metadata
        size_pipe = add_size_metadata
        time_pipe_fd = add_time_metadata
    time_pipe = partial(time_pipe_fd, time_metric=time_metric)

    pipes = [
        *([partial(filter_out_pattern, pattern=hide)] if hide else []),
        # Add symlink and directory bools from Path methods
        *([permissions_pipe, owner_group_pipe] if l else []),
        add_path_metadata,
        *([add_symlink_targets] if l and not L else []),
        *([size_pipe] if S or l else []),
        *([time_pipe] if l or (t or implied_time_sort) else []),
        *([append_slash] if p else []),
        *([] if U else sort_pipes),
        # Post-sort
        *([make_size_human_readable] if h and (S or l) else []),
        *([make_size_si_unit] if si and (S or l) else []),
    ]

    results = []
    failures = []
    for idx, path_set in enumerate((individual_files, *dirs_to_scan)):
        is_dir = idx > 0
        special_ss = False
        if not path_set:
            assert idx == 0  # This should only be when no files
            continue
        if is_dir:
            # Use source string "" for individual files in working dir and "." for WD
            # itself except if there are no individual files/other dirs to scan then
            # WD source string becomes "" (identical behaviour to `ls`)
            dir_root_s = str(path_set)
            no_files = len(individual_files) == 0
            no_more_dirs = len(dirs_to_scan) == 1
            # Special case for printing a single directory without its path
            special_ss = dir_root_s == "." and no_files and no_more_dirs and not R
            if special_ss:
                dir_root_s = ""
            dir_root = path_set
            drrp = dir_root._raw_paths
            is_dot_rel = drrp and drrp[0].split(os.path.sep, 1)[0] == "."
            path_set = [
                *([Path("."), Path("..")] if a and not A else []),
            ]
            for path_set_file in dir_root.iterdir():
                if hidden_files_allowed or not path_set_file.name.startswith("."):
                    try:
                        # Just do this to try to trigger an OSError to discard it early
                        path_set_file.is_file()
                    except OSError as e:
                        if error_to != devnull:
                            print(
                                f"pols: cannot access '{path_set_file}': {e}",
                                file=error_to,
                            )
                        if raise_on_access:
                            raise
                        continue
                    else:
                        subpath = path_set_file.relative_to(dir_root)
                        rs_subpath = resegment_raw_path(
                            Path(
                                os.path.sep.join([*dir_root._raw_paths, *subpath.parts])
                            )
                        )
                        path_set.append(rs_subpath)
        else:
            dir_root_s = ""
            dir_root = Path(dir_root_s)
            subpaths = path_set
            drrp = dir_root._raw_paths
            is_dot_rel = drrp and drrp[0].split(os.path.sep, 1)[0] == "."
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
            files = pl.LazyFrame(
                file_entries,
                schema={"path": pl.Object, "name": pl.String, "rel_to": pl.Object},
            )
        except Exception as e:
            failures.extend([ValueError(f"Got no files from {path_set} due to {e}"), e])
            if raise_on_access:
                raise e
            else:
                if error_to != devnull:
                    print(e, file=error_to)
            continue
        path_set_result = reduce(pl.LazyFrame.pipe, pipes, files).drop(drop_cols)

        source_string = (
            dir_root_s
            if (is_dot_rel and special_ss) or (not dir_root.name)
            else os.path.sep.join(drrp)
        )
        if merge_all:
            results.append({source_string: path_set_result})
        else:
            results.append({source_string: path_set_result.collect()})
    if merge_all:
        merger = []
        for item in results:
            merge_el_source, merge_el_df = next(iter(item.items()))
            merge_el_with_src = merge_el_df.with_columns(source=pl.lit(merge_el_source))
            merger.append(merge_el_with_src)
        merged = pl.concat(merger)
        if with_filter is not None:
            try:
                merged = merged.filter(
                    eval(with_filter)  # evaluate to Expr
                    if (
                        isinstance(with_filter, str)
                        and with_filter not in merged.columns
                    )
                    else with_filter  # either Expr already or column name
                )
            except Exception as e:
                filter_fail_msg = f"Filter {with_filter!r} failed, skipped"
                raise ValueError(filter_fail_msg) from e
        merged = merged.collect()
    if print_to != devnull:
        if merge_all:
            print(merged, file=print_to)
        else:
            for result in results:
                [(source, paths)] = result.items()
                if source:
                    print(f"{source}:", file=print_to)
                print(paths, file=print_to)
    if inspect:
        breakpoint()
    if to_dict:
        if merge_all:
            return {"": merged}
        else:
            return {source: df for res in results for source, df in res.items()}
    elif to_dicts:
        return results
    else:
        return None

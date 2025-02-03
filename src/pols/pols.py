from __future__ import annotations

import os.path
import re
from functools import partial, reduce
from pathlib import Path
from sys import argv, stderr, stdout
from typing import TYPE_CHECKING, Callable, Literal, TypeAlias

import polars as pl

from .features.hide import filter_out_pattern
from .features.p import append_slash
from .features.v import numeric_sort
from .resegment import resegment_raw_path
from .walk import walk_root_rel_raw_paths

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
        Literal[
            "atime", "access", "use", "ctime", "status", "birth", "creation", "mtime"
        ]
    ) = "mtime",
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
    to_dict: bool = False,
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
         with `l`: show ctime and sort by name;
         otherwise: sort by ctime, newest first.
      [x] d: List directories themselves, not their contents.
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
      [x] r: Reverse order while sorting.
      [ ] R: List directories recursively.
      [ ] S: Sort by file size, largest first.
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
      [x] keep_path: Keep a path column with the Pathlib path object.
      [x] keep_fs_metadata: Keep filesystem metadata booleans: `is_dir`, `is_symlink`.
      [x] print_to: Where to print to, by default writes to STDOUT, `None` to disable.
      [x] error_to: Where to error to, by default writes to STDERR, `None` to disable.
      [x] to_dict: Return the result as dict (key is the source: for individual files
          the source is the current path `"."`, for directory contents it is the parent
          directory).
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

    TOFIX:
    - `S` flag does not seem to work correctly, change to a function and unpack paths
      manually to create new column with values.
    """
    if to_dict and to_dicts:
        raise ValueError("Please pass only one of `to_dict` and `to_dicts`.")
    # Handle short codes
    hide = hide or I
    hidden_files_allowed = A or a
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
            globbed_paths = list(glob_base.glob(glob_subpattern))
            expanded_paths.extend(globbed_paths)
        else:
            expanded_paths.append(path)

    for path in expanded_paths:
        try:
            if not path.exists():
                raise FileNotFoundError(f"No such file or directory")
            is_file = path.is_file()
        except OSError as e:
            # This includes FileNotFoundError we threw as well as access errors
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
        excs = ExceptionGroup(f"No such file:", nonexistent)
        if raise_on_access:
            raise excs
        else:
            print(excs, file=error_to)

    if R:
        for unscanned_dir in dirs_to_scan[:]:
            if unscanned_dir.is_absolute():
                # Simple case, can use `pathlib.Path.walk()`
                descendant_dirs = [dir_p for dir_p, _, _ in unscanned_dir.walk()][1:]
            else:
                # Construct from parts for the `walk_root_rel_raw_paths` function
                raw_unscanned_dir = resegment_raw_path(unscanned_dir)
                usd_R = walk_root_rel_raw_paths(raw_unscanned_dir, report=debug)[1:]
                # Must preserve raw paths carefully. Flatten sublevel lists first
                desc_dir_strs = [dir_p for dir_level in usd_R for dir_p in dir_level]
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
    if implied_time_sort := (c or u) and ((not l) or (t and l)):
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
        # Sort in reverse order of specification so sorts given first are applied last
        for sort_flag, sort_val in reversed(sort_sequence.items()):
            sort_desc = False
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
                case "t" | "u" | "c":
                    sort_by = "time"
                    sort_desc = True
                case "v":
                    sort_by = numeric_sort(pl.col("name"))
                case "X":
                    sort_by = pl.col("name").str.split(".").list.last()
                case _:
                    raise ValueError(f"Invalid flag in sort sequence {sort_flag}")

            sort_func = lambda df: df.sort(
                by=sort_by, maintain_order=True, descending=sort_desc
            )
            sort_pipes.append(sort_func)
    else:
        lexico_sort = lambda df: df.sort(
            by=pl.col("name").str.to_lowercase(), maintain_order=True
        )
        sort_pipes.append(lexico_sort)
    if r and not U:
        sort_pipes.append(lambda df: df.reverse())

    pipes = [
        *([partial(filter_out_pattern, pattern=hide)] if hide else []),
        # Add symlink and directory bools from Path methods
        add_path_metadata,
        *(
            [partial(add_time_metadata, time_metric=time_metric)]
            if t or implied_time_sort
            else []
        ),
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
            # Use source string "" for individual files in working dir and "." for WD
            # itself except if there are no individual files/other dirs to scan then
            # WD source string becomes "" (identical behaviour to `ls`)
            dir_root_s = str(path_set)
            no_files = len(individual_files) == 0
            no_more_dirs = len(dirs_to_scan) == 1
            if dir_root_s == "." and no_files and no_more_dirs and not R:
                dir_root_s = ""
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
            dir_root_s = ""
            dir_root = Path(dir_root_s)
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
            files = pl.DataFrame(
                file_entries,
                schema={"path": pl.Object, "name": pl.String, "rel_to": pl.Object},
            )
        except Exception as e:
            failures.extend([ValueError(f"Got no files from {path_set} due to {e}"), e])
            if raise_on_access:
                raise e
            else:
                print(e, file=error_to)
            continue
        path_set_result = reduce(pl.DataFrame.pipe, pipes, files).drop(drop_cols)
        # print(path_set_result.to_dicts())
        source_string = str(dir_root) if dir_root.name else dir_root_s
        results.append({source_string: path_set_result})
    if print_to:
        for result in results:
            [(source, paths)] = result.items()
            if source:
                print(f"{source}:", file=print_to)
            print(paths, file=print_to)
            if debug:
                breakpoint()
    if to_dict:
        return {source: df for res in results for source, df in res.items()}
    elif to_dicts:
        return results
    else:
        return None


def add_path_metadata(files):
    pth = pl.col("path")
    return files.with_columns(
        is_dir=pth.map_elements(lambda p: p.is_dir(), return_dtype=pl.Boolean),
        is_symlink=pth.map_elements(lambda p: p.is_symlink(), return_dtype=pl.Boolean),
    )


def add_time_metadata(files, time_metric: Literal["atime", "ctime", "mtime"]):
    time_stat = f"st_{time_metric}"
    return files.with_columns(
        time=pl.col("path")
        .map_elements(lambda p: getattr(p.stat(), time_stat), return_dtype=pl.Float64)
        .mul(1000)
        .cast(pl.Datetime("ms")),
    )


# Known bugs: `pols *` in home dir fails, maybe permissions error?

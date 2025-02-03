import os.path
import re
from pathlib import Path


def walk_root_rel_raw_paths(source: Path, report: bool = True):
    """
    Walk a path's subtree of paths (descendants), restoring the `.` prefix for all of
    these directories' raw paths (the 0'th raw path) so as to preserve this information
    (which is stripped out by the `pathlib.Path.walk()` algorithm after the 1st step).

    The `_raw_paths` slot stored on `pathlib.Path` objects tells you whether it is
    truly a `.`-relative path like `./foo/hello.py` or a relative path without the
    leading `.` directory like `foo/hello.py`. Pathlib usually hides ("normalises")
    this, but you need it to distinguish `Path()` from `Path("")` and `Path(".")` which
    can be important in some contexts, as noted in the pathlib docs.

    If you control the `Path` creation (in particular, whether the Path is made from
    parts) then you can control the creation of this initial 'raw path' (which is
    essentially what was input to the base `PurePath` class's `__init__` method, i.e.
    the string you call `Path()` with, or lack thereof. If we can control the raw path,
    we can amend it with a simple find and replace. N.B. will not work if the input raw
    path contains `os.path.sep` (check/recreate if needed before calling this function).

    Assuming it came from a `Path` instantiated from parts, not a single path-separated
    string, the 0'th element in `_raw_paths` will be the top directory part with the `.`
    """
    if report:
        print(f"Processing source path {source!r} -> {source._raw_paths=}")
    prefix = source.parts[0] if source.parts else ""
    return [
        [
            re.sub(
                pattern=f"^{re.escape(prefix)}",
                repl=(
                    source._raw_paths[0].removesuffix(os.path.sep)
                    + (
                        ""
                        if source.parts  # Non-`.` (or equivalently) source path
                        else (os.path.sep if source._raw_paths == ["."] else "")
                    )
                ),
                string=(
                    parent_dir._raw_paths[0]
                    + (
                        os.path.sep.join(["", *source._raw_paths[1:]])
                        if source._raw_paths == parent_dir._raw_paths
                        else ""
                    )
                ),
                count=1,
            )
            if source._raw_paths and parent_dir._raw_paths != ["."]
            else raw_path_item
            for raw_path_item in parent_dir._raw_paths[:1]
        ]
        for parent_dir, _, _ in source.walk()
    ]


if __name__ == "__main__":
    """The root has a `./` base directory which gets lost when normalised."""
    Path(".")

    """The path repr, str, and parts all lack it."""
    [str(parent_dir) for parent_dir, _, _ in root.walk()]
    # ['.', 'foo', 'foo/bar', 'foo/bar/baz']

    """The `_raw_paths: list[str]` slot stores it, but `walk()` loses it by the 2nd step."""
    [parent_dir._raw_paths[0] for parent_dir, _, _ in root.walk()]
    # ['.', 'foo', 'foo/bar', 'foo/bar/baz']

    """
    Notice: `walk()` is making the paths as a single string, so `_raw_paths` is a singleton
    list, unlike when made from parts. This means it's easy to manipulate as a string.
    """
    (Path("path") / "to" / "file")._raw_paths
    # ['path', 'to', 'file']
    Path("path/to/file")._raw_paths
    # ['path/to/file']

    answers = """\
    [[], ['foo'], ['foo/bar'], ['foo/bar/baz']]
    [[''], ['foo'], ['foo/bar'], ['foo/bar/baz']]
    [['.'], ['./foo'], ['./foo/bar'], ['./foo/bar/baz']]
    [['foo'], ['foo/bar'], ['foo/bar/baz']]
    [['./foo'], ['./foo/bar'], ['./foo/bar/baz']]
    [['foo/bar'], ['foo/bar/baz']]
    [['./foo/bar'], ['./foo/bar/baz']]
    [['foo/bar/baz']]
    [['./foo/bar/baz']]
    [['foo/'], ['foo/bar'], ['foo/bar/baz']]
    [['./foo/'], ['./foo/bar'], ['./foo/bar/baz']]
    [['foo/bar/'], ['foo/bar/baz']]
    [['./foo/bar/'], ['./foo/bar/baz']]
    """.splitlines()
    a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13 = map(eval, answers)

    PF = ["\N{CROSS MARK}", "\N{WHITE HEAVY CHECK MARK}"]
    print(1, (a := walk_root_rel_raw_paths(Path())), PF[a == a1], end="\n\n")
    print(2, (a := walk_root_rel_raw_paths(Path(""))), PF[a == a2], end="\n\n")
    print(3, (a := walk_root_rel_raw_paths(Path("."))), PF[a == a3], end="\n\n")
    print(4, (a := walk_root_rel_raw_paths(Path("foo"))), PF[a == a4], end="\n\n")
    print(
        5,
        (a := walk_root_rel_raw_paths(Path("./foo"))),
        PF[a == a5],
        end="\n\n",
    )
    print(
        6,
        (a := walk_root_rel_raw_paths(Path("foo") / "bar")),
        PF[a == a6],
        end="\n\n",
    )
    print(
        7,
        (a := walk_root_rel_raw_paths(Path("./foo") / "bar")),
        PF[a == a7],
        end="\n\n",
    )
    print(
        8,
        (a := walk_root_rel_raw_paths(Path("foo") / "bar" / "baz")),
        PF[a == a8],
        end="\n\n",
    )
    print(
        9,
        (a := walk_root_rel_raw_paths(Path("./foo") / "bar" / "baz")),
        PF[a == a9],
        end="\n\n",
    )
    print(
        10,
        (a := walk_root_rel_raw_paths(Path("foo/"))),
        PF[a == a10],
        end="\n\n",
    )
    print(
        11,
        (a := walk_root_rel_raw_paths(Path("./foo/"))),
        PF[a == a11],
        end="\n\n",
    )
    print(
        12,
        (a := walk_root_rel_raw_paths(Path("foo") / "bar/")),
        PF[a == a12],
        end="\n\n",
    )
    print(
        13,
        (a := walk_root_rel_raw_paths(Path("./foo") / "bar/")),
        PF[a == a13],
        end="\n\n",
    )

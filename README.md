# polars-ls

List directory contents as Polars DataFrames.

## Installation

The `polars-ls` package can be installed with either `polars` or `polars-lts-cpu` using the extras
by those names:

```bash
pip install polars-ls[polars]
pip install polars-ls[polars-lts-cpu]
```

If Polars is already installed, you can simply `pip install polars-ls`.

## User guidance

### Names are relative

Counter to the typical `pathlib.Path` notion of a name, the names in `ls` and hence `pols` are
more relative names: hence `.` is a valid name (if you try accessing the `.name` attribute of a
pathlib Path, it'll come back as "").

```python
>>> cwd = Path.cwd()
>>> cwd / "."
PosixPath('/home/louis/dev/polars-ls')
>>> cwd / ".."
PosixPath('/home/louis/dev/polars-ls/..')
>>> (cwd / ".").name
'polars-ls'
>>> (cwd / "..").name
'..'
>>> Path(".").name
''
```

### Individual files and directories don't mix

The way `ls` works is that individual files get collected in one 'set' of results and directories
in another, and never the two shall meet. If you `ls` a few files and one or more directories,
you'll get one set of reults with all the files and one set for each of the folders. This is
because of the previous point: the names shown are relative to the directory 'root' (if you're
specifying files individually, the current working directory is the assumed directory 'root', and
of course absolute paths always show as absolute so their 'root' is shown too).

(Even if the individual files are in different folders: it's because merging files with
different roots whose relative names are being shown would be invalid)

```bash
$ ls README.md src src/pols/__init__.py 
README.md  src/pols/__init__.py

src:
pols
```

To the same effect, the results are grouped in a list of dicts, where the key is the source
(either the empty string for the individual files, or the directory root). This allows an identical
printout style to `ls`:

```bash
$ ls -A ../.py*
../.python-version

../.pytest_cache:
CACHEDIR.TAG  .gitignore  README.md  v
$ pols -A ../.py*
shape: (1, 1)
┌────────────────────┐
│ name               │
│ ---                │
│ str                │
╞════════════════════╡
│ ../.python-version │
└────────────────────┘
../.pytest_cache:
shape: (4, 1)
┌──────────────┐
│ name         │
│ ---          │
│ str          │
╞══════════════╡
│ README.md    │
│ v            │
│ .gitignore   │
│ CACHEDIR.TAG │
└──────────────┘
```

### Globs (Kleene stars) go 1 level deep

You can use `**` in `ls` and `pols` but in both cases you only actually get one level, unlike other
tools (and Python's glob).

```bash
$ ls src/pols/**.py
src/pols/cli.py  src/pols/__init__.py  src/pols/pols.py
$ ls src/pols/*/*.py
src/pols/features/a.py  src/pols/features/A.py  src/pols/features/hide.py
src/pols/features/__init__.py  src/pols/features/p.py
```

### Patterns that don't match will error non-fatally

It's allowed to not match a file, just like in `ls`:

```bash
$ ls *.yaml *.toml *.md
ls: cannot access '*.yaml': No such file or directory
 pyproject.toml   README.md

$ pols *.yaml *.toml *.md
pols: cannot access '*.yaml': No such file or directory
shape: (2, 1)
┌────────────────┐
│ name           │
│ ---            │
│ str            │
╞════════════════╡
│ pyproject.toml │
│ README.md      │
└────────────────┘
```

### `OSError`s like `FileNotFoundError` are non-fatal but can be thrown with `raise_on_access`

If you want such errors to be fatal, pass `raise_on_acecss` (`--raise-on-access` on the command line):

```bash
$ pols *.yaml *.toml *.md --raise-on-access
pols: cannot access '*.yaml': No such file or directory
Traceback (most recent call last):
...
FileNotFoundError: No such file or directory
```

Note that the file expansion and preparation is done before any printing or DataFrame operations, so
these errors won't occur mid-way through any Polars computations.

### Sorting is applied in the same order given

> (Note: so far this only applies for the command line)

Just like `ls`, command line order affects `pols` for sorting flags.
The sorts are applied in order their flags are given, setting the Polars `.sort(maintain_order=True)`
parameter.

See [Polars docs](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.sort.html) for more information.

```bash
$ ls -St
pols.py  features  walk.py  resegment.py  cli.py  __init__.py
$ pols --S --t
shape: (6, 1)
┌──────────────┐
│ name         │
│ ---          │
│ str          │
╞══════════════╡
│ pols.py      │
│ features     │
│ walk.py      │
│ resegment.py │
│ __init__.py  │
│ cli.py       │
└──────────────┘
$ ls -tS
pols.py  walk.py  features  resegment.py  cli.py  __init__.py
$ pols --t --S
shape: (6, 1)
┌──────────────┐
│ name         │
│ ---          │
│ str          │
╞══════════════╡
│ pols.py      │
│ walk.py      │
│ features     │
│ resegment.py │
│ cli.py       │
│ __init__.py  │
└──────────────┘
```

## Differences from `ls`

The design is intended to keep as closely as possible to GNU coreutils
[`ls`](https://www.gnu.org/software/coreutils/ls).

### 1. `hide` is not disabled by `a`/`A`

Another is that `hide` is not disabled by `a`/`A` because there is no need to, and this enables
filtering hidden files minus some pattern. In `ls`, `--hide` silently fails if passed with `-a`.

### 2. Name sort order is standard lexicographic sort

One other is that lexicographic name sort is different: I am just using regular Polars sort, `ls` appears
to do one where `_` is ignored, compare:

```bash
$ ls -l
total 84
-rw-rw-r-- 1 louis louis    81 Feb  1 19:13 cli.py
drwxrwxr-x 2 louis louis  4096 Feb  3 16:05 features
-rw-rw-r-- 1 louis louis    23 Feb  1 19:13 __init__.py
-rw-rw-r-- 1 louis louis 20138 Feb  3 15:54 pols.py
-rw-rw-r-- 1 louis louis   651 Feb  3 03:19 resegment.py
-rw-rw-r-- 1 louis louis  6543 Feb  3 12:43 walk.py
$ pols --l
shape: (6, 6)
┌──────────────┬─────────────┬───────┬───────┬───────┬─────────────────────────┐
│ name         ┆ permissions ┆ owner ┆ group ┆ size  ┆ time                    │
│ ---          ┆ ---         ┆ ---   ┆ ---   ┆ ---   ┆ ---                     │
│ str          ┆ str         ┆ str   ┆ str   ┆ i64   ┆ datetime[ms]            │
╞══════════════╪═════════════╪═══════╪═══════╪═══════╪═════════════════════════╡
│ __init__.py  ┆ -rw-rw-r--  ┆ louis ┆ louis ┆ 23    ┆ 2025-02-01 19:13:57.460 │
│ cli.py       ┆ -rw-rw-r--  ┆ louis ┆ louis ┆ 81    ┆ 2025-02-01 19:13:57.460 │
│ features     ┆ drwxrwxr-x  ┆ louis ┆ louis ┆ 4096  ┆ 2025-02-03 16:05:02.225 │
│ pols.py      ┆ -rw-rw-r--  ┆ louis ┆ louis ┆ 20138 ┆ 2025-02-03 15:54:40.182 │
│ resegment.py ┆ -rw-rw-r--  ┆ louis ┆ louis ┆ 651   ┆ 2025-02-03 03:19:02.034 │
│ walk.py      ┆ -rw-rw-r--  ┆ louis ┆ louis ┆ 6543  ┆ 2025-02-03 12:43:07.437 │
└──────────────┴─────────────┴───────┴───────┴───────┴─────────────────────────┘
```

I personally prefer the second one and expect it is more in line with expectations anyway, so I'm
leaving it that way.

## Extra features

### `inspect`

Want to look at the actual results not just command line print outs? The `inspect` parameter will
drop you into a PDB debugger to quickly be able to handle the data itself.

Try passing lists of column names as the comma-separated values to `drop_only` and
`keep` to change the final columns available for inspection.

### `merge_all` to DataFrame

Passing the `merge_all` flag will collect all of the results in a single DataFrame, with the
directory sources becoming a `source` column.

### Filtering with `with_filter`

Allows passing either:

- a column name, just like in normal Polars filter expressions, which get evaluated to an implicit
  "rows where column's value is True" filter.
- a string which will `eval` to a Polars `Expr`
- a Polars `Expr` (when using as a Python library)

To avoid invalid results from filtering out entire directories yet still having them in the results
dictionary, `with_filter` always implies `merge_all`.

### `as_path`

The `as_path` parameter (`--as-path` flag) gives the result back as a `pathlib.Path` type, Polars object
dtype column 'path', instead of the name str type, Polars string dtype column 'name'. Obviously this
makes no difference on the command line!

```bash
$ pols
shape: (1, 1)
┌──────┐
│ name │
│ ---  │
│ str  │
╞══════╡
│ pols │
└──────┘
$ pols --as-path
shape: (1, 1)
┌────────┐
│ path   │
│ ---    │
│ object │
╞════════╡
│ pols   │
└────────┘
```

## `merge_all`

As the name suggests, all source directories get merged into a single DataFrame, which is printed as
normal. The `source` column is added to preserve the directory each row came from.

For example, here is the recursive listing of the `src/pols` path in this repo:

```bash
$ ls -R
.:
pols

./pols:
cli.py  features  __init__.py  pols.py  resegment.py  walk.py

./pols/features:
hide.py  h.py  __init__.py  p.py  S.py  v.py
```

Here it is in a merged DataFrame:

```bash
$ pols -R --merge-all
shape: (13, 2)
┌───────────────────────┬─────────────────┐
│ name                  ┆ source          │
│ ---                   ┆ ---             │
│ str                   ┆ str             │
╞═══════════════════════╪═════════════════╡
│ pols                  ┆ .               │
│ pols/__init__.py      ┆ ./pols          │
│ pols/cli.py           ┆ ./pols          │
│ pols/features         ┆ ./pols          │
│ pols/pols.py          ┆ ./pols          │
│ …                     ┆ …               │
│ pols/features/h.py    ┆ ./pols/features │
│ pols/features/hide.py ┆ ./pols/features │
│ pols/features/p.py    ┆ ./pols/features │
│ pols/features/S.py    ┆ ./pols/features │
│ pols/features/v.py    ┆ ./pols/features │
└───────────────────────┴─────────────────┘
```

If used in combination with `to_dict`, the merged DataFrame is stored in the single key,
the empty string key.

> Note: passing `--to-dict` returns the value, so to avoid seeing the print out and the result dict
> to show the merge result we pass `--print-to="devnull"` to turn off printing.

```bash
$ pols -R --merge-all --to-dict --print-to="devnull"
{'': shape: (13, 2)
┌───────────────────────┬─────────────────┐
│ name                  ┆ source          │
│ ---                   ┆ ---             │
│ str                   ┆ str             │
╞═══════════════════════╪═════════════════╡
│ pols                  ┆ .               │
│ pols/__init__.py      ┆ ./pols          │
│ pols/cli.py           ┆ ./pols          │
│ pols/features         ┆ ./pols          │
│ pols/pols.py          ┆ ./pols          │
│ …                     ┆ …               │
│ pols/features/h.py    ┆ ./pols/features │
│ pols/features/hide.py ┆ ./pols/features │
│ pols/features/p.py    ┆ ./pols/features │
│ pols/features/S.py    ┆ ./pols/features │
│ pols/features/v.py    ┆ ./pols/features │
└───────────────────────┴─────────────────┘}
```

### `drop_only` and `keep`

As well as the `ls -l` style interface, the `drop_only` parameter (`--drop-only` in
the CLI) will allow you to specify columns to keep, for more control and for ease of debugging.

These are flags to include/exclude computed columns from being dropped. Typically, we don't discard
columns when we compute them, but the underlying goal of this tool is to imitate `ls`, so we must.
To see all the information `pols` collects, set `drop_only` to `""` (i.e. the empty list as a
comma-separated string).

```bash
$ pols
.:
shape: (6, 1)
┌────────────────┐
│ name           │
│ ---            │
│ str            │
╞════════════════╡
│ dist           │
│ pyproject.toml │
│ README.md      │
│ src            │
│ tests          │
│ uv.lock        │
└────────────────┘
$ pols --drop-only ''
.:
shape: (6, 5)
┌────────────────┬────────────────┬────────┬────────┬────────────┐
│ path           ┆ name           ┆ rel_to ┆ is_dir ┆ is_symlink │
│ ---            ┆ ---            ┆ ---    ┆ ---    ┆ ---        │
│ object         ┆ str            ┆ object ┆ bool   ┆ bool       │
╞════════════════╪════════════════╪════════╪════════╪════════════╡
│ dist           ┆ dist           ┆ .      ┆ true   ┆ false      │
│ pyproject.toml ┆ pyproject.toml ┆ .      ┆ false  ┆ false      │
│ README.md      ┆ README.md      ┆ .      ┆ false  ┆ false      │
│ src            ┆ src            ┆ .      ┆ true   ┆ false      │
│ tests          ┆ tests          ┆ .      ┆ true   ┆ false      │
│ uv.lock        ┆ uv.lock        ┆ .      ┆ false  ┆ false      │
└────────────────┴────────────────┴────────┴────────┴────────────┘
$ pols --t --drop-only ''
.:
shape: (6, 6)
┌────────────────┬────────────────┬────────┬────────┬────────────┬─────────────────────────┐
│ path           ┆ name           ┆ rel_to ┆ is_dir ┆ is_symlink ┆ time                    │
│ ---            ┆ ---            ┆ ---    ┆ ---    ┆ ---        ┆ ---                     │
│ object         ┆ str            ┆ object ┆ bool   ┆ bool       ┆ datetime[ms]            │
╞════════════════╪════════════════╪════════╪════════╪════════════╪═════════════════════════╡
│ README.md      ┆ README.md      ┆ .      ┆ false  ┆ false      ┆ 2025-02-03 14:30:19.458 │
│ dist           ┆ dist           ┆ .      ┆ true   ┆ false      ┆ 2025-02-03 14:13:09.173 │
│ pyproject.toml ┆ pyproject.toml ┆ .      ┆ false  ┆ false      ┆ 2025-02-03 14:12:54.917 │
│ uv.lock        ┆ uv.lock        ┆ .      ┆ false  ┆ false      ┆ 2025-02-02 12:33:52.007 │
│ src            ┆ src            ┆ .      ┆ true   ┆ false      ┆ 2025-02-01 19:13:57.460 │
│ tests          ┆ tests          ┆ .      ┆ true   ┆ false      ┆ 2025-02-01 19:13:57.460 │
└────────────────┴────────────────┴────────┴────────┴────────────┴─────────────────────────┘
$ pols -R --merge-all --keep 'is_dir'
shape: (28, 3)
┌─────────────────────────┬────────┬─────────┐
│ name                    ┆ is_dir ┆ source  │
│ ---                     ┆ ---    ┆ ---     │
│ str                     ┆ bool   ┆ str     │
╞═════════════════════════╪════════╪═════════╡
│ dist                    ┆ true   ┆ .       │
│ pyproject.toml          ┆ false  ┆ .       │
│ README.md               ┆ false  ┆ .       │
│ src                     ┆ true   ┆ .       │
│ tests                   ┆ true   ┆ .       │
│ …                       ┆ …      ┆ …       │
│ tests/core_test.py      ┆ false  ┆ ./tests │
│ tests/format_test.py    ┆ false  ┆ ./tests │
│ tests/recursion_test.py ┆ false  ┆ ./tests │
│ tests/sort_test.py      ┆ false  ┆ ./tests │
│ tests/symlink_test.py   ┆ false  ┆ ./tests │
└─────────────────────────┴────────┴─────────┘
```

Naturally there is also a `keep` parameter (`--keep` flag) (which will prevent the named columns from being dropped).

```bash
$ pols --t
.:
shape: (6, 1)
┌────────────────┐
│ name           │
│ ---            │
│ str            │
╞════════════════╡
│ README.md      │
│ dist           │
│ pyproject.toml │
│ uv.lock        │
│ src            │
│ tests          │
└────────────────┘
$ pols --t --keep path
.:
shape: (6, 2)
┌────────────────┬────────────────┐
│ path           ┆ name           │
│ ---            ┆ ---            │
│ object         ┆ str            │
╞════════════════╪════════════════╡
│ README.md      ┆ README.md      │
│ dist           ┆ dist           │
│ pyproject.toml ┆ pyproject.toml │
│ uv.lock        ┆ uv.lock        │
│ src            ┆ src            │
│ tests          ┆ tests          │
└────────────────┴────────────────┘
$ pols --t --keep time
.:
shape: (6, 2)
┌────────────────┬─────────────────────────┐
│ name           ┆ time                    │
│ ---            ┆ ---                     │
│ str            ┆ datetime[ms]            │
╞════════════════╪═════════════════════════╡
│ README.md      ┆ 2025-02-03 14:38:01.979 │
│ dist           ┆ 2025-02-03 14:13:09.173 │
│ pyproject.toml ┆ 2025-02-03 14:12:54.917 │
│ uv.lock        ┆ 2025-02-02 12:33:52.007 │
│ src            ┆ 2025-02-01 19:13:57.460 │
│ tests          ┆ 2025-02-01 19:13:57.460 │
└────────────────┴─────────────────────────┘
$ pols --t --keep'path,time'
.:
shape: (6, 3)
┌────────────────┬────────────────┬─────────────────────────┐
│ path           ┆ name           ┆ time                    │
│ ---            ┆ ---            ┆ ---                     │
│ object         ┆ str            ┆ datetime[ms]            │
╞════════════════╪════════════════╪═════════════════════════╡
│ README.md      ┆ README.md      ┆ 2025-02-03 14:38:01.979 │
│ dist           ┆ dist           ┆ 2025-02-03 14:13:09.173 │
│ pyproject.toml ┆ pyproject.toml ┆ 2025-02-03 14:12:54.917 │
│ uv.lock        ┆ uv.lock        ┆ 2025-02-02 12:33:52.007 │
│ src            ┆ src            ┆ 2025-02-01 19:13:57.460 │
│ tests          ┆ tests          ┆ 2025-02-01 19:13:57.460 │
└────────────────┴────────────────┴─────────────────────────┘
```

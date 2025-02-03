import os.path
import re
from pathlib import Path

sep = re.escape(os.path.sep)
# Regex to take 1st dir: `./a` or `a` or `.`, no match for empty str
pat = re.compile(rf"^(?:\.{sep})*([^{sep}]+)")


def resegment_raw_path(raw_dir: Path) -> Path:
    """
    Split a path into its top-level component and the rest, then join as
    segments. Returns the new path with separated raw paths.
    """
    dir_raw = os.path.join(*raw_dir._raw_paths)
    pat_match = re.match(pat, dir_raw)
    raw_top_dir = Path(pat_match.group() if pat_match else ".")
    dir_rel_rest = raw_dir.relative_to(raw_top_dir)
    return raw_top_dir.joinpath(*dir_rel_rest.parts)

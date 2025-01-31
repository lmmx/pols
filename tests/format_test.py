import pytest
from freezegun import freeze_time
from inline_snapshot import snapshot
from pols import pols


@freeze_time("2025-01-31 12:00:00")
def test_long_format_with_iso_time(test_dir):
    """Test the known working format: A=True, l=True, r=True, time_style="long-iso"."""
    result = pols(test_dir, A=True, l=True, r=True, time_style="long-iso")
    assert str(result) == snapshot(
        """\
shape: (6, 2)
┌─────────────┬─────────────────────────┐
│ name        ┆ mtime                   │
│ ---         ┆ ---                     │
│ str         ┆ datetime[ms]            │
╞═════════════╪═════════════════════════╡
│ .hidden.txt ┆ 2025-01-31 12:00:00     │
│ file1.txt   ┆ 2025-01-31 12:00:00     │
│ file2.txt   ┆ 2025-01-31 12:00:00     │
│ script.py   ┆ 2025-01-31 12:00:00     │
│ link.txt    ┆ 2025-01-31 12:00:00     │
│ subdir      ┆ 2025-01-31 23:24:40.893 │
└─────────────┴─────────────────────────┘\
"""
    )  # Let the test generate the actual output


@pytest.mark.xfail(reason="Other time styles not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_other_time_styles(test_dir):
    """Test other time style formats."""
    result = pols(test_dir, l=True, time_style="full-iso")
    assert str(result) == snapshot()

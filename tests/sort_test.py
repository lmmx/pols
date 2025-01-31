import pytest
from freezegun import freeze_time
from inline_snapshot import snapshot
from pols import pols


@pytest.mark.xfail(reason="Size sorting not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_size_sort(test_dir):
    """Test sorting by file size."""
    result = pols(test_dir, S=True)
    assert str(result) == snapshot()


@pytest.mark.xfail(reason="Extension sorting not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_extension_sort(test_dir):
    """Test sorting by extension."""
    result = pols(test_dir, X=True)
    assert str(result) == snapshot()

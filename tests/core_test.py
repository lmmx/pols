import pytest
from freezegun import freeze_time
from inline_snapshot import snapshot
from pols import pols


@pytest.mark.xfail(reason="Basic listing not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_basic_listing(test_dir):
    """Test basic directory listing without any flags."""
    result = pols(test_dir)
    assert str(result) == snapshot()


@pytest.mark.xfail(reason="Hidden files not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_hidden_files(test_dir):
    """Test listing with hidden files (--a flag)."""
    result = pols(test_dir, a=True)
    assert str(result) == snapshot()

import pytest
from freezegun import freeze_time
from inline_snapshot import snapshot
from pols import pols


@pytest.mark.xfail(reason="Symlink handling not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_symlink_follow(test_dir):
    """Test following symlinks."""
    result = pols(test_dir, L=True)
    assert str(result) == snapshot()

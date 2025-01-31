import pytest
from freezegun import freeze_time
from inline_snapshot import snapshot
from pols import pols


@pytest.mark.xfail(reason="Recursive listing not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_recursive(test_dir):
    """Test recursive directory listing."""
    result = pols(test_dir, R=True)
    assert str(result) == snapshot()

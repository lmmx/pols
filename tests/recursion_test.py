import pytest
from freezegun import freeze_time
from inline_snapshot import snapshot
from pols import ls


@pytest.mark.xfail(reason="Recursive listing not implemented yet")
@freeze_time("2025-01-31 12:00:00")
def test_recursive(test_dir):
    """Test recursive directory listing."""
    result = ls(test_dir, R=True)
    assert str(result) == snapshot()

import os
from datetime import datetime
import pytest
from freezegun import freeze_time

# Use a consistent timestamp for all tests: 2025-01-31 12:00:00 UTC
TEST_TIME = "2025-01-31 12:00:00"
TEST_TIMESTAMP = int(datetime.fromisoformat(TEST_TIME).timestamp())


@pytest.fixture
def test_dir(tmp_path):
    """Create a test directory with various files and subdirectories."""
    with freeze_time(TEST_TIME):
        # Create files with consistent timestamps
        files = [
            "file1.txt",
            "file2.txt",
            ".hidden.txt",
            "script.py",
        ]

        # Create all main files
        for filename in files:
            path = tmp_path / filename
            path.write_text(f"Content of {filename}")
            os.utime(path, (TEST_TIMESTAMP, TEST_TIMESTAMP))

        # Create subdirectory FIRST and set its timestamp IMMEDIATELY
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        os.utime(
            subdir, (TEST_TIMESTAMP, TEST_TIMESTAMP)
        )  # Set timestamp right after creation

        # Then create files in subdirectory
        subdir_files = [
            "subfile1.txt",
            "subfile2.py",
        ]

        for filename in subdir_files:
            path = subdir / filename
            path.write_text(f"Content of {filename}")
            os.utime(path, (TEST_TIMESTAMP, TEST_TIMESTAMP))
    return subdir

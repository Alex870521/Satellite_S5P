"""
Basic tests for the Sentinel-5P processor.
These tests don't require any data files.
"""
import pytest


def test_import():
    """Test that the package can be imported."""
    try:
        import src
        assert True
    except ImportError:
        assert False, "Failed to import the package"


def test_basic():
    """A basic test that always passes."""
    assert True


@pytest.mark.requires_data
def test_with_data():
    """This test requires data and will be skipped."""
    # 這個測試會被跳過，因為它有 requires_data 標記
    pytest.skip("This test requires data files")
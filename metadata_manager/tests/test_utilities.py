import pytest
from md_utils import count_line_changes


@pytest.mark.e35aeef590
@pytest.mark.utils
@pytest.mark.sanity
def test_count_line_changes():
    old_hashes = ["a", "b", "c"]
    new_hashes = ["c", "b", "a"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = []
    new_hashes = ["a", "b", "c"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 3
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a", "b", "c"]
    new_hashes = []
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 3

    old_hashes = ["a", "b"]
    new_hashes = ["a", "b", "c", "d"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 2
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a", "b", "c", "d"]
    new_hashes = ["a", "b"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 2

    old_hashes = ["a", "b", "b"]
    new_hashes = ["a", "b", "b", "c", "c", "c"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 3
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a", "b", "b", "c", "c", "c"]
    new_hashes = ["a", "b", "b"]
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 3

    old_hashes = []
    new_hashes = []
    assert count_line_changes(old_hashes, new_hashes).lines_added == 0
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 0

    old_hashes = ["a"] * 1_000_000
    new_hashes = ["a"] * 500_000 + ["b"] * 500_000
    assert count_line_changes(old_hashes, new_hashes).lines_added == 500_000
    assert count_line_changes(old_hashes, new_hashes).lines_removed == 500_000

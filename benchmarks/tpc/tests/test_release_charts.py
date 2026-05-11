import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_charts import replace_marker_region


def test_replace_marker_region_replaces_between_markers():
    text = (
        "intro\n"
        "<!-- AUTO-GENERATED:charts:START -->\n"
        "old content\n"
        "<!-- AUTO-GENERATED:charts:END -->\n"
        "outro\n"
    )
    result = replace_marker_region(text, "charts", "new content\n")
    assert result == (
        "intro\n"
        "<!-- AUTO-GENERATED:charts:START -->\n"
        "new content\n"
        "<!-- AUTO-GENERATED:charts:END -->\n"
        "outro\n"
    )


def test_replace_marker_region_preserves_other_markers():
    text = (
        "<!-- AUTO-GENERATED:foo:START -->\nA\n<!-- AUTO-GENERATED:foo:END -->\n"
        "between\n"
        "<!-- AUTO-GENERATED:charts:START -->\nB\n<!-- AUTO-GENERATED:charts:END -->\n"
    )
    result = replace_marker_region(text, "charts", "B2\n")
    assert "A\n<!-- AUTO-GENERATED:foo:END -->" in result
    assert "B2\n<!-- AUTO-GENERATED:charts:END -->" in result


def test_replace_marker_region_missing_start_raises():
    text = "no markers here\n<!-- AUTO-GENERATED:charts:END -->\n"
    with pytest.raises(ValueError, match="charts:START"):
        replace_marker_region(text, "charts", "x")


def test_replace_marker_region_missing_end_raises():
    text = "<!-- AUTO-GENERATED:charts:START -->\nstuff\n"
    with pytest.raises(ValueError, match="charts:END"):
        replace_marker_region(text, "charts", "x")

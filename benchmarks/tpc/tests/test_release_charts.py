import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_charts import classify_conf


def test_classify_conf_partitions_by_value():
    spark = {
        "spark.executor.cores": "8",
        "spark.executor.memory": "16g",
        "spark.driver.memory": "8G",
    }
    comet = {
        "spark.executor.cores": "8",
        "spark.executor.memory": "32g",
        "spark.driver.memory": "8G",
        "spark.comet.scan.impl": "native_datafusion",
    }
    common, spark_only, comet_only = classify_conf(spark, comet)

    assert common == [
        ("spark.driver.memory", "8G"),
        ("spark.executor.cores", "8"),
    ]
    assert spark_only == [("spark.executor.memory", "16g")]
    assert comet_only == [
        ("spark.comet.scan.impl", "native_datafusion"),
        ("spark.executor.memory", "32g"),
    ]


def test_classify_conf_drops_noise_keys():
    noise = {
        "spark.driver.extraJavaOptions": "...",
        "spark.executor.extraJavaOptions": "...",
        "spark.driver.extraClassPath": "/tmp/x.jar",
        "spark.executor.extraClassPath": "/tmp/x.jar",
        "spark.driver.host": "10.0.0.1",
        "spark.driver.port": "1234",
        "spark.executor.id": "driver",
        "spark.app.id": "app-1",
        "spark.app.name": "x",
        "spark.app.startTime": "0",
        "spark.master": "spark://h:7077",
        "spark.jars": "/tmp/y.jar",
        "spark.submit.deployMode": "client",
        "spark.repl.class.uri": "...",
    }
    common, spark_only, comet_only = classify_conf(dict(noise), dict(noise))
    assert common == []
    assert spark_only == []
    assert comet_only == []


def test_classify_conf_sorted_by_key():
    spark = {"spark.z": "1", "spark.a": "1", "spark.m": "1"}
    comet = {"spark.z": "1", "spark.a": "1", "spark.m": "1"}
    common, _, _ = classify_conf(spark, comet)
    assert [k for k, _ in common] == ["spark.a", "spark.m", "spark.z"]


from release_charts import render_conf_tables


def test_render_conf_tables_basic():
    common = [("spark.executor.cores", "8")]
    spark_only = [("spark.executor.memory", "16g")]
    comet_only = [
        ("spark.comet.scan.impl", "native_datafusion"),
        ("spark.executor.memory", "32g"),
    ]
    result = render_conf_tables(common, spark_only, comet_only)
    assert result == (
        "### Common\n"
        "\n"
        "| Property | Value |\n"
        "| --- | --- |\n"
        "| spark.executor.cores | 8 |\n"
        "\n"
        "### Spark\n"
        "\n"
        "| Property | Value |\n"
        "| --- | --- |\n"
        "| spark.executor.memory | 16g |\n"
        "\n"
        "### Comet\n"
        "\n"
        "| Property | Value |\n"
        "| --- | --- |\n"
        "| spark.comet.scan.impl | native_datafusion |\n"
        "| spark.executor.memory | 32g |\n"
    )


def test_render_conf_tables_empty_sections_use_none_placeholder():
    result = render_conf_tables([], [("spark.x", "1")], [])
    assert "### Common\n\n_None._\n" in result
    assert "### Comet\n\n_None._\n" in result
    assert "| spark.x | 1 |" in result


def test_render_conf_tables_escapes_pipe_in_value():
    result = render_conf_tables([("spark.x", "a|b")], [], [])
    assert "| spark.x | a\\|b |" in result


import pytest

from release_charts import replace_marker_region


def test_replace_marker_region_replaces_between_markers():
    text = (
        "intro\n"
        "<!-- AUTO-GENERATED:config:START -->\n"
        "old content\n"
        "<!-- AUTO-GENERATED:config:END -->\n"
        "outro\n"
    )
    result = replace_marker_region(text, "config", "new content\n")
    assert result == (
        "intro\n"
        "<!-- AUTO-GENERATED:config:START -->\n"
        "new content\n"
        "<!-- AUTO-GENERATED:config:END -->\n"
        "outro\n"
    )


def test_replace_marker_region_preserves_other_markers():
    text = (
        "<!-- AUTO-GENERATED:config:START -->\nA\n<!-- AUTO-GENERATED:config:END -->\n"
        "between\n"
        "<!-- AUTO-GENERATED:charts:START -->\nB\n<!-- AUTO-GENERATED:charts:END -->\n"
    )
    result = replace_marker_region(text, "charts", "B2\n")
    assert "A\n<!-- AUTO-GENERATED:config:END -->" in result
    assert "B2\n<!-- AUTO-GENERATED:charts:END -->" in result


def test_replace_marker_region_missing_start_raises():
    text = "no markers here\n<!-- AUTO-GENERATED:config:END -->\n"
    with pytest.raises(ValueError, match="config:START"):
        replace_marker_region(text, "config", "x")


def test_replace_marker_region_missing_end_raises():
    text = "<!-- AUTO-GENERATED:config:START -->\nstuff\n"
    with pytest.raises(ValueError, match="config:END"):
        replace_marker_region(text, "config", "x")

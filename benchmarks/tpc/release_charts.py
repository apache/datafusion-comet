# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

NOISE_KEYS = frozenset({
    "spark.driver.extraJavaOptions",
    "spark.executor.extraJavaOptions",
    "spark.driver.extraClassPath",
    "spark.executor.extraClassPath",
    "spark.driver.host",
    "spark.driver.port",
    "spark.executor.id",
    "spark.app.id",
    "spark.app.name",
    "spark.app.startTime",
    "spark.master",
    "spark.jars",
})

NOISE_PREFIXES = ("spark.submit.", "spark.repl.")


def _is_noise(key: str) -> bool:
    if key in NOISE_KEYS:
        return True
    return any(key.startswith(p) for p in NOISE_PREFIXES)


def _filter(conf: dict) -> dict:
    return {k: v for k, v in conf.items() if not _is_noise(k)}


def classify_conf(spark_conf: dict, comet_conf: dict) -> tuple[list, list, list]:
    spark = _filter(spark_conf)
    comet = _filter(comet_conf)

    common = []
    spark_only = []
    comet_only = []

    for key in sorted(set(spark) | set(comet)):
        in_spark = key in spark
        in_comet = key in comet
        if in_spark and in_comet and spark[key] == comet[key]:
            common.append((key, spark[key]))
        else:
            if in_spark:
                spark_only.append((key, spark[key]))
            if in_comet:
                comet_only.append((key, comet[key]))

    return common, spark_only, comet_only


def _escape_md_cell(value: str) -> str:
    return value.replace("|", "\\|")


def _render_table(rows):
    if not rows:
        return "_None._\n"
    lines = ["| Property | Value |", "| --- | --- |"]
    for key, value in rows:
        lines.append(f"| {key} | {_escape_md_cell(str(value))} |")
    return "\n".join(lines) + "\n"


def render_conf_tables(common, spark_only, comet_only) -> str:
    parts = []
    for heading, rows in [("Common", common), ("Spark", spark_only), ("Comet", comet_only)]:
        parts.append(f"### {heading}\n\n{_render_table(rows)}")
    return "\n".join(parts)


def replace_marker_region(text: str, name: str, new_content: str) -> str:
    start = f"<!-- AUTO-GENERATED:{name}:START -->"
    end = f"<!-- AUTO-GENERATED:{name}:END -->"

    start_idx = text.find(start)
    if start_idx < 0:
        raise ValueError(f"missing marker {start}")
    end_idx = text.find(end, start_idx + len(start))
    if end_idx < 0:
        raise ValueError(f"missing marker {end} after {name}:START")

    body_start = start_idx + len(start)
    if not new_content.startswith("\n"):
        new_content = "\n" + new_content
    if not new_content.endswith("\n"):
        new_content = new_content + "\n"

    return text[:body_start] + new_content + text[end_idx:]


import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BENCHMARKS = [
    {"name": "tpch", "label": "TPC-H", "md": "tpc-h.md"},
    {"name": "tpcds", "label": "TPC-DS", "md": "tpc-ds.md"},
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _render_charts_block(bench: str, version: str) -> str:
    lines = []
    for base in [f"{bench}_allqueries.png", f"{bench}_queries_compare.png", f"{bench}_queries_speedup_rel.png", f"{bench}_queries_speedup_abs.png"]:
        lines.append(f"![](../../_static/images/benchmark-results/{version}/{base})\n")
    return "\n".join(lines)


def _process_benchmark(bench, version, spark_version, title_suffix, repo_root):
    results_dir = repo_root / "benchmarks" / "results" / version
    spark_json = results_dir / f"spark-{bench['name']}.json"
    comet_json = results_dir / f"comet-{bench['name']}.json"

    if not spark_json.exists() or not comet_json.exists():
        logger.warning(
            "skipping %s: need both %s and %s",
            bench["label"], spark_json, comet_json,
        )
        return

    image_dir = repo_root / "docs" / "source" / "_static" / "images" / "benchmark-results" / version
    image_dir.mkdir(parents=True, exist_ok=True)

    title = f"{bench['label']} {title_suffix}"
    spark_label = f"Spark {spark_version}"
    comet_label = f"Comet {version}"

    cmd = [
        sys.executable,
        str(Path(__file__).parent / "generate-comparison.py"),
        str(spark_json),
        str(comet_json),
        "--labels", spark_label, comet_label,
        "--benchmark", bench["name"],
        "--title", title,
        "--output-dir", str(image_dir),
    ]
    logger.info("generating %s charts -> %s", bench["label"], image_dir)
    subprocess.run(cmd, check=True)

    with open(spark_json) as f:
        spark_conf = json.load(f).get("spark_conf", {})
    with open(comet_json) as f:
        comet_conf = json.load(f).get("spark_conf", {})

    common, spark_only, comet_only = classify_conf(spark_conf, comet_conf)
    conf_md = render_conf_tables(common, spark_only, comet_only)
    charts_md = _render_charts_block(bench["name"], version)

    md_path = repo_root / "docs" / "source" / "contributor-guide" / "benchmark-results" / bench["md"]
    text = md_path.read_text()
    try:
        text = replace_marker_region(text, "config", conf_md)
        text = replace_marker_region(text, "charts", charts_md)
    except ValueError as e:
        logger.error("%s: %s", md_path, e)
        logger.error("add the AUTO-GENERATED:config and AUTO-GENERATED:charts marker pairs first")
        raise SystemExit(1)
    md_path.write_text(text)
    logger.info("updated %s", md_path)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Regenerate release benchmark charts and markdown.")
    parser.add_argument("version", help="Comet release version, e.g. 0.17.0")
    parser.add_argument("--spark-version", required=True, help="Spark version used as baseline, e.g. 3.5.8")
    parser.add_argument("--title-suffix", default="@ SF1000 (1TB)", help="Appended to TPC-H / TPC-DS in chart titles")
    args = parser.parse_args(argv)

    repo_root = _repo_root()
    for bench in BENCHMARKS:
        _process_benchmark(bench, args.version, args.spark_version, args.title_suffix, repo_root)


if __name__ == "__main__":
    main()

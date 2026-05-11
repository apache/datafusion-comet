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

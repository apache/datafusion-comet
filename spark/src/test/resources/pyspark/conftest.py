#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Shared fixtures and helpers for the pytest modules under this directory and
for the benchmark scripts that import them.

The `spark` and `accelerated` fixtures are shared by every UDF test module, so
one Spark session (and one JVM) serves the whole run.

`resolve_comet_jar` returns the path to the Comet jar a Spark session needs.
Resolution order: the `COMET_JAR` env var (taken verbatim if it points at a
file, expanded as a glob otherwise), then `<repo>/spark/target` matched against
the installed pyspark major.minor version.
"""

import glob
import os

import pytest
from pyspark.sql import SparkSession


REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)


def resolve_comet_jar() -> str:
    explicit = os.environ.get("COMET_JAR")
    if explicit:
        if any(ch in explicit for ch in "*?["):
            matches = sorted(glob.glob(explicit))
            if not matches:
                raise FileNotFoundError(
                    f"COMET_JAR pattern matched nothing: {explicit}"
                )
            return matches[-1]
        return explicit

    # Pick the jar that matches the installed pyspark major.minor version. The
    # Comet jars are published per Spark version (e.g.
    # comet-spark-spark3.5_2.12-*.jar); using the wrong one yields
    # ClassNotFoundException on Scala stdlib classes.
    import pyspark

    major_minor = ".".join(pyspark.__version__.split(".")[:2])
    spark_tag = f"spark{major_minor}"
    scala_tag = "_2.12" if major_minor.startswith("3.") else "_2.13"
    # Match any version suffix, not just `-SNAPSHOT`: on a release branch the
    # Maven version is the final release version (e.g. `1.0.0`) with no
    # `-SNAPSHOT` qualifier.
    pattern = os.path.join(
        REPO_ROOT,
        f"spark/target/comet-spark-{spark_tag}{scala_tag}-*.jar",
    )
    candidates = [
        m
        for m in sorted(glob.glob(pattern))
        if not any(
            tag in os.path.basename(m)
            for tag in ("sources", "tests", "javadoc", "shaded")
        )
    ]
    if not candidates:
        raise FileNotFoundError(
            "Comet jar not found. Set COMET_JAR or run `make release`. "
            f"Looked under {pattern}."
        )
    return candidates[-1]


@pytest.fixture(scope="session")
def spark():
    jar = resolve_comet_jar()
    # PYSPARK_SUBMIT_ARGS is consumed when pyspark launches its JVM. Setting
    # --jars puts the Comet jar on both driver and executor classpaths so the
    # CometPlugin can be loaded.
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {jar} --driver-class-path {jar} pyspark-shell"
    )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("comet-python-udf-tests")
        .config("spark.plugins", "org.apache.spark.CometPlugin")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        # spark.comet.shuffle.enabled defaults to true, and
        # CometSparkSessionExtensions.isCometLoaded refuses to register Comet's rules
        # at all when shuffle is on but spark.shuffle.manager is not the Comet manager.
        # These tests do not need Comet shuffle, so disable it explicitly to keep
        # Comet's scan and exec rules active without configuring shuffle.
        .config("spark.comet.shuffle.enabled", "false")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture(params=[True, False], ids=["accelerated", "fallback"])
def accelerated(request, spark) -> bool:
    spark.conf.set(
        "spark.comet.exec.pyarrowUDF.enabled",
        "true" if request.param else "false",
    )
    return request.param


def executed_plan(df) -> str:
    return df._jdf.queryExecution().executedPlan().toString()

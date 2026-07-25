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
Shared helpers for the pytest modules under this directory and for the
benchmark scripts that import them.

`resolve_comet_jar` returns the path to the Comet jar a Spark session needs.
Resolution order: the `COMET_JAR` env var (taken verbatim if it points at a
file, expanded as a glob otherwise), then `<repo>/spark/target` matched against
the installed pyspark major.minor version.
"""

import glob
import os


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
    pattern = os.path.join(
        REPO_ROOT,
        f"spark/target/comet-spark-{spark_tag}{scala_tag}-*-SNAPSHOT.jar",
    )
    candidates = [
        m
        for m in sorted(glob.glob(pattern))
        if "sources" not in os.path.basename(m) and "tests" not in os.path.basename(m)
    ]
    if not candidates:
        raise FileNotFoundError(
            "Comet jar not found. Set COMET_JAR or run `make release`. "
            f"Looked under {pattern}."
        )
    return candidates[-1]

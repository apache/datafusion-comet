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

"""Verify that Spark-distributed files keep scalar Arrow UDFs on Python workers."""

import os

import pyarrow as pa
import pytest
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.pandas.functions import arrow_udf

from conftest import resolve_comet_jar


@pytest.fixture
def spark():
    jar = resolve_comet_jar()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {jar} --driver-class-path {jar} pyspark-shell"
    )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("comet-native-arrow-udf-files")
        .config("spark.plugins", "org.apache.spark.CometPlugin")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        .config("spark.comet.shuffle.enabled", "false")
        .config("spark.comet.exec.nativeArrowPythonUDF.enabled", "true")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


@pytest.mark.parametrize("file_kind", ["python", "data"])
def test_spark_added_files_use_python_worker(spark, tmp_path, file_kind):
    assert spark.sparkContext._jsc.sc().listFiles().size() == 0
    if file_kind == "python":
        helper = tmp_path / "comet_arrow_udf_helper.py"
        helper.write_text("def offset():\n    return 7\n")
        spark.sparkContext.addPyFile(str(helper))

        @arrow_udf("long")
        def add_offset(values):
            import comet_arrow_udf_helper

            return pa.array(
                [value + comet_arrow_udf_helper.offset() for value in values.to_pylist()],
                type=pa.int64(),
            )

    else:
        data = tmp_path / "comet_arrow_udf_offset.txt"
        data.write_text("7\n")
        spark.sparkContext.addFile(str(data))

        @arrow_udf("long")
        def add_offset(values):
            with open(SparkFiles.get("comet_arrow_udf_offset.txt")) as source:
                offset = int(source.read())
            return pa.array(
                [value + offset for value in values.to_pylist()], type=pa.int64()
            )

    assert spark.sparkContext._jsc.sc().listFiles().size() == 1
    result = spark.range(2).select(add_offset("id"))
    plan = result._jdf.queryExecution().executedPlan().toString()
    assert "ArrowEvalPython" in plan
    assert "CometArrowEvalPython" not in plan
    assert [row[0] for row in result.collect()] == [7, 8]

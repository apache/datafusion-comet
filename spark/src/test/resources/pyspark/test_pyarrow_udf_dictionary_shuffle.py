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

"""Regression coverage for dictionary-encoded Comet shuffle input to Python UDFs."""

import os

import pyarrow as pa
import pytest
from conftest import resolve_comet_jar
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    jar = resolve_comet_jar()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {jar} --driver-class-path {jar} pyspark-shell"
    )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("comet-pyarrow-udf-dictionary-shuffle-tests")
        .config("spark.plugins", "org.apache.spark.CometPlugin")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        .config("spark.comet.exec.pyarrowUDF.enabled", "true")
        .config(
            "spark.shuffle.manager",
            "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
        )
        .config("spark.comet.shuffle.mode", "jvm")
        .config("spark.comet.shuffle.jvm.preferDictionary.ratio", "1.01")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _comparable(row):
    return (
        row.id,
        row.text,
        None if row.data is None else bytes(row.data),
    )


@pytest.mark.parametrize("api", ["mapInArrow", "mapInPandas"])
def test_dictionary_shuffle_input(spark, tmp_path, api: str):
    spark.conf.set("spark.sql.execution.arrow.useLargeVarTypes", "false")
    rows = []
    for index in range(200):
        text = None if index % 23 == 0 else ("" if index % 17 == 0 else "same-text")
        data = (
            None
            if index % 29 == 0
            else (bytearray() if index % 19 == 0 else bytearray(b"same-binary"))
        )
        rows.append((index, text, data))

    path = str(tmp_path / "dictionary-shuffle.parquet")
    spark.createDataFrame(rows, "id int, text string, data binary").write.parquet(path)
    source = spark.read.parquet(path).repartition(2, "id")

    if api == "mapInArrow":

        def passthrough(iterator):
            for batch in iterator:
                text_type = batch.schema.field("text").type
                data_type = batch.schema.field("data").type
                assert pa.types.is_string(text_type)
                assert pa.types.is_binary(data_type)
                yield batch

        result = source.mapInArrow(passthrough, source.schema)
    else:

        def passthrough(iterator):
            yield from iterator

        result = source.mapInPandas(passthrough, source.schema)

    plan = result._jdf.queryExecution().executedPlan().toString()
    assert "CometColumnarExchange" in plan, plan
    assert "CometMapInBatch" in plan, plan
    assert "ColumnarToRow" not in plan, plan

    actual = sorted(_comparable(row) for row in result.collect())
    expected = sorted(
        (
            index,
            text,
            None if data is None else bytes(data),
        )
        for index, text, data in rows
    )
    assert actual == expected

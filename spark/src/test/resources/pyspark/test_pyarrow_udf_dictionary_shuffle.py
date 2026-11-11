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
from pyspark.sql import SparkSession, types as T


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
    index, text, data = row[:3]
    return index, text, None if data is None else bytes(data)


@pytest.mark.parametrize("api", ["mapInArrow", "mapInPandas"])
@pytest.mark.parametrize(
    "max_records,max_bytes,expected_batch_sizes",
    [
        pytest.param(10000, 256 * 1024 * 1024, None, id="nulls-and-empty-values"),
        pytest.param(2, 256 * 1024 * 1024, [2] * 5, id="record-limit"),
        pytest.param(100, 4096, [1] * 10, id="decoded-byte-limit"),
    ],
)
def test_dictionary_shuffle_input(
    spark, tmp_path, api, max_records, max_bytes, expected_batch_sizes
):
    """Preserve logical types/values and split by records or decoded dictionary size."""
    settings = {
        "spark.sql.execution.arrow.useLargeVarTypes": "false",
        "spark.sql.execution.arrow.maxRecordsPerBatch": str(max_records),
        "spark.sql.execution.arrow.maxBytesPerBatch": str(max_bytes),
    }
    previous = {key: spark.conf.get(key) for key in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        if expected_batch_sizes is None:
            rows = [
                (
                    index,
                    None if index % 23 == 0 else ("" if index % 17 == 0 else "same-text"),
                    None
                    if index % 29 == 0
                    else (b"" if index % 19 == 0 else b"same-binary"),
                )
                for index in range(200)
            ]
        else:
            rows = [
                (
                    index,
                    ("a" if index % 2 else "b") * 32768,
                    (b"c" if index % 2 else b"d") * 32768,
                )
                for index in range(10)
            ]
        path = str(tmp_path / "dictionary-shuffle.parquet")
        spark.createDataFrame(rows, "id int, text string, data binary").coalesce(
            1
        ).write.parquet(path)
        partitions = 2 if expected_batch_sizes is None else 1
        source = spark.read.parquet(path).repartition(partitions, "id")
        output_schema = T.StructType(
            [
                *source.schema.fields,
                T.StructField("input_batch_id", T.IntegerType(), nullable=False),
                T.StructField("input_batch_rows", T.IntegerType(), nullable=False),
            ]
        )

        if api == "mapInArrow":

            def annotate_batches(iterator):
                for batch_id, batch in enumerate(iterator):
                    assert pa.types.is_string(batch.schema.field("text").type)
                    assert pa.types.is_binary(batch.schema.field("data").type)
                    yield pa.RecordBatch.from_arrays(
                        [
                            *batch.columns,
                            pa.array([batch_id] * batch.num_rows, type=pa.int32()),
                            pa.array([batch.num_rows] * batch.num_rows, type=pa.int32()),
                        ],
                        names=output_schema.fieldNames(),
                    )

        else:

            def annotate_batches(iterator):
                for batch_id, frame in enumerate(iterator):
                    yield frame.assign(
                        input_batch_id=batch_id, input_batch_rows=len(frame)
                    )

        result = getattr(source, api)(annotate_batches, output_schema)
        plan = result._jdf.queryExecution().executedPlan().toString()
        assert "CometColumnarExchange" in plan, plan
        assert "CometMapInBatch" in plan, plan
        assert "ColumnarToRow" not in plan, plan

        output = result.collect()
        assert sorted(map(_comparable, output)) == sorted(map(_comparable, rows))
        if expected_batch_sizes is not None:
            observed_batches = {}
            for row in output:
                observed_batches.setdefault(row.input_batch_id, []).append(row)
            assert sorted(observed_batches) == list(range(len(expected_batch_sizes)))
            for batch_id, size in enumerate(expected_batch_sizes):
                batch_rows = observed_batches[batch_id]
                assert len(batch_rows) == size
                assert {row.input_batch_rows for row in batch_rows} == {size}
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)

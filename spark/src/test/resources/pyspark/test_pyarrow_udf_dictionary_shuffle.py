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


@pytest.mark.parametrize("api", ["mapInArrow", "mapInPandas"])
@pytest.mark.parametrize(
    "max_records,max_bytes,expected_batch_sizes",
    [
        (2, 256 * 1024 * 1024, [2, 2, 2, 2, 2]),
        (100, 4096, [1] * 10),
    ],
)
def test_dictionary_shuffle_input_respects_arrow_batch_limits(
    spark,
    tmp_path,
    api: str,
    max_records: int,
    max_bytes: int,
    expected_batch_sizes: list[int],
):
    """Split compact shuffle dictionaries using their decoded logical size."""
    previous_records = spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch")
    previous_bytes = spark.conf.get("spark.sql.execution.arrow.maxBytesPerBatch")
    spark.conf.set("spark.sql.execution.arrow.useLargeVarTypes", "false")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(max_records))
    spark.conf.set("spark.sql.execution.arrow.maxBytesPerBatch", str(max_bytes))
    try:
        text_values = ["a" * (32 * 1024), "b" * (32 * 1024)]
        binary_values = [bytearray(b"c" * (32 * 1024)), bytearray(b"d" * (32 * 1024))]
        rows = [
            (index, text_values[index % 2], binary_values[index % 2])
            for index in range(10)
        ]

        path = str(tmp_path / "dictionary-shuffle-batch-limits.parquet")
        spark.createDataFrame(rows, "id int, text string, data binary").coalesce(
            1
        ).write.parquet(path)
        source = spark.read.parquet(path).repartition(1, "id")
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
                    yield pa.RecordBatch.from_arrays(
                        [
                            *batch.columns,
                            pa.array([batch_id] * batch.num_rows, type=pa.int32()),
                            pa.array(
                                [batch.num_rows] * batch.num_rows, type=pa.int32()
                            ),
                        ],
                        names=output_schema.fieldNames(),
                    )

            result = source.mapInArrow(annotate_batches, output_schema)
        else:

            def annotate_batches(iterator):
                for batch_id, frame in enumerate(iterator):
                    yield frame.assign(
                        input_batch_id=batch_id,
                        input_batch_rows=len(frame),
                    )

            result = source.mapInPandas(annotate_batches, output_schema)

        plan = result._jdf.queryExecution().executedPlan().toString()
        assert "CometColumnarExchange" in plan, plan
        assert "CometMapInBatch" in plan, plan
        assert "ColumnarToRow" not in plan, plan

        output = result.collect()
        observed_batches = {}
        for row in output:
            observed_batches.setdefault(row.input_batch_id, []).append(row)
        assert sorted(observed_batches) == list(range(len(expected_batch_sizes)))
        assert [
            len(observed_batches[batch_id]) for batch_id in sorted(observed_batches)
        ] == expected_batch_sizes
        for batch_rows in observed_batches.values():
            assert {row.input_batch_rows for row in batch_rows} == {len(batch_rows)}

        actual = sorted(_comparable(row) for row in output)
        expected = sorted((index, text, bytes(data)) for index, text, data in rows)
        assert actual == expected
    finally:
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", previous_records)
        spark.conf.set("spark.sql.execution.arrow.maxBytesPerBatch", previous_bytes)

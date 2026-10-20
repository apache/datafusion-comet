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

"""Seeded data generation for PyArrow UDF fuzz coverage (issue #4384).

The generator check runs without starting Spark. End-to-end tests consume the
same cases through Parquet and a real Python worker in both execution modes.
"""

import datetime as dt
import inspect
import os
import random
from decimal import Decimal

import pyarrow as pa
import pytest
from conftest import resolve_comet_jar
from pyspark.sql import Row, SparkSession, types as T
from pyspark.sql.pandas.types import to_arrow_schema


SEEDS = (4384, 4234, 5368)
NULL_FRACTIONS = (0.0, 0.01, 0.5, 0.99, 1.0)
MAX_DEPTH = 3
COLLECTION_LENGTHS = (0, 1, 2, 7)
LARGE_ARRAY_LENGTH = 128
VARIABLE_LENGTHS = (0, 1, 7, 31, 1024)


@pytest.fixture(scope="session")
def spark():
    jar = resolve_comet_jar()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {jar} --driver-class-path {jar} pyspark-shell"
    )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("comet-pyarrow-udf-fuzz-tests")
        .config("spark.plugins", "org.apache.spark.CometPlugin")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        # These tests scan one Parquet partition and do not require shuffle.
        .config("spark.comet.shuffle.enabled", "false")
        # Spark writes signed small integers. Allow ShortType scans despite the
        # conservative guard for Parquet files containing unsigned UINT_8.
        .config("spark.comet.scan.unsignedSmallIntSafetyCheck", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _primitive_types(rng):
    precision = rng.choice((1, 9, 18, 19, 28, 38))
    return [
        T.BooleanType(),
        T.ByteType(),
        T.ShortType(),
        T.IntegerType(),
        T.LongType(),
        T.FloatType(),
        T.DoubleType(),
        T.StringType(),
        T.BinaryType(),
        T.DateType(),
        T.TimestampType(),
        T.TimestampNTZType(),
        T.DecimalType(precision, rng.randint(0, precision)),
    ]


def _random_type(rng, depth, kind=None):
    if depth == 0:
        return rng.choice(_primitive_types(rng))
    kind = kind or rng.choice(("primitive", "array", "struct", "map"))
    if kind == "primitive":
        return rng.choice(_primitive_types(rng))
    if kind == "array":
        return T.ArrayType(_random_type(rng, depth - 1), containsNull=True)
    if kind == "struct":
        return T.StructType(
            [
                T.StructField(f"f{i}", _random_type(rng, depth - 1), nullable=True)
                for i in range(rng.randint(1, 3))
            ]
        )
    if kind == "map":
        # Simple key types let us preserve map values as Python dictionaries.
        return T.MapType(
            rng.choice((T.StringType(), T.LongType())),
            _random_type(rng, depth - 1),
            valueContainsNull=True,
        )
    raise ValueError(f"Unknown type category: {kind}")


def _random_value(rng, data_type, null_fraction):
    if rng.random() < null_fraction:
        return None

    if isinstance(data_type, T.BooleanType):
        return bool(rng.getrandbits(1))
    for integer_type, bits in (
        (T.ByteType, 8),
        (T.ShortType, 16),
        (T.IntegerType, 32),
        (T.LongType, 64),
    ):
        if isinstance(data_type, integer_type):
            minimum, maximum = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
            return rng.choice((minimum, maximum, 0, rng.randint(minimum, maximum)))
    if isinstance(data_type, (T.FloatType, T.DoubleType)):
        # Binary fractions in this range are exact even in float32. This keeps
        # input rounding separate from the transport correctness being tested.
        return rng.randint(-(1 << 20), 1 << 20) / 4.0
    if isinstance(data_type, T.DecimalType):
        maximum = 10**data_type.precision - 1
        unscaled = rng.choice((0, maximum, rng.randint(0, maximum)))
        digits = tuple(int(digit) for digit in str(unscaled))
        # Tuple construction is exact, including precision 38, regardless of
        # Python's decimal context (whose default precision is only 28).
        return Decimal((rng.getrandbits(1), digits, -data_type.scale))
    if isinstance(data_type, T.StringType):
        return "".join(
            rng.choice("aZ09 é中😀") for _ in range(rng.choice(VARIABLE_LENGTHS))
        )
    if isinstance(data_type, T.BinaryType):
        return rng.randbytes(rng.choice(VARIABLE_LENGTHS))
    if isinstance(data_type, T.DateType):
        return dt.date(2000, 1, 1) + dt.timedelta(days=rng.randint(-20000, 20000))
    if isinstance(data_type, (T.TimestampType, T.TimestampNTZType)):
        return dt.datetime(2000, 1, 1) + dt.timedelta(
            days=rng.randint(-20000, 20000),
            seconds=rng.randrange(86400),
            microseconds=rng.randrange(1000000),
        )
    if isinstance(data_type, T.ArrayType):
        lengths = COLLECTION_LENGTHS
        # Keep outer containers small so large lengths do not multiply
        # at every nesting level.
        if not isinstance(
            data_type.elementType, (T.ArrayType, T.StructType, T.MapType)
        ):
            lengths = (*lengths, LARGE_ARRAY_LENGTH)
        return [
            _random_value(rng, data_type.elementType, null_fraction)
            for _ in range(rng.choice(lengths))
        ]
    if isinstance(data_type, T.StructType):
        return {
            field.name: _random_value(rng, field.dataType, null_fraction)
            for field in data_type.fields
        }
    if isinstance(data_type, T.MapType):
        # Sampling without replacement keeps keys unique and non-null.
        keys = rng.sample(range(10000), rng.choice(COLLECTION_LENGTHS))
        if isinstance(data_type.keyType, T.StringType):
            keys = [f"key_{key}" for key in keys]
        return {
            key: _random_value(rng, data_type.valueType, null_fraction) for key in keys
        }
    raise TypeError(f"Unsupported generated type: {data_type}")

def _generate_case(seed, num_rows=37, null_fraction=None):
    rng = random.Random(seed)
    # Each case includes every primitive category plus each container category;
    # recursive shapes, decimal types, column order and values depend on the seed.
    data_types = _primitive_types(rng)
    data_types.extend(
        _random_type(rng, MAX_DEPTH, kind) for kind in ("array", "struct", "map")
    )
    rng.shuffle(data_types)
    fields = [T.StructField("row_id", T.LongType(), nullable=False)]
    fields.extend(
        T.StructField(f"c{i}", data_type) for i, data_type in enumerate(data_types)
    )
    schema = T.StructType(fields)
    null_fractions = [
        rng.choice(NULL_FRACTIONS) if null_fraction is None else null_fraction
        for _ in data_types
    ]
    rows = []
    for row_id in range(num_rows):
        row = {"row_id": row_id}
        for field, fraction in zip(fields[1:], null_fractions):
            row[field.name] = _random_value(rng, field.dataType, fraction)
        rows.append(row)
    return schema, rows


@pytest.mark.parametrize("seed", SEEDS, ids=lambda seed: f"seed-{seed}")
def test_generated_data_arrow_ipc_roundtrip(seed):
    schema, rows = _generate_case(seed)
    context = f"seed={seed}, schema={schema.json()}"
    assert _generate_case(seed) == (schema, rows), context

    # Use UTC for TimestampType; TimestampNTZType remains timezone-naive.
    # PySpark 4.2 replaced timestamp_utc with timezone.
    if "timezone" in inspect.signature(to_arrow_schema).parameters:
        arrow_schema = to_arrow_schema(schema, timezone="UTC")
    else:
        arrow_schema = to_arrow_schema(schema, timestamp_utc=True)
    table = pa.Table.from_pylist(rows, schema=arrow_schema)
    table.validate(full=True)
    output = pa.BufferOutputStream()
    with pa.ipc.new_stream(output, arrow_schema) as writer:
        writer.write_table(table, max_chunksize=7)
    with pa.ipc.open_stream(output.getvalue()) as reader:
        restored = reader.read_all()
    restored.validate(full=True)
    assert restored.schema == arrow_schema, context
    # Strict conversion also rejects any duplicate Arrow map keys; comparing
    # ordered rows (with unique IDs) preserves multiplicity instead of using sets.
    assert restored.to_pylist(maps_as_pydicts="strict") == table.to_pylist(
        maps_as_pydicts="strict"
    ), context


def _normalize(value):
    if isinstance(value, Row):
        value = value.asDict()
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, bytearray):
        return bytes(value)
    return value


def _assert_rows_equal(actual, expected, context):
    actual = sorted((_normalize(row) for row in actual), key=lambda row: row["row_id"])
    expected = sorted((_normalize(row) for row in expected), key=lambda row: row["row_id"])
    # Check the complete ID sequence, including multiplicity, before comparing
    # fields. Sorting only IDs avoids imposing an ordering on nested maps.
    assert [row["row_id"] for row in actual] == [
        row["row_id"] for row in expected
    ], context
    for actual_row, expected_row in zip(actual, expected):
        assert actual_row.keys() == expected_row.keys(), context
        for name, value in expected_row.items():
            assert actual_row[name] == value, (
                f"{context}, row_id={expected_row['row_id']}, column={name}"
            )


@pytest.mark.parametrize("seed", SEEDS, ids=lambda seed: f"seed-{seed}")
@pytest.mark.parametrize("batch_size", (7, 16), ids=lambda size: f"batch-{size}")
@pytest.mark.parametrize(
    "null_fraction",
    (None, *NULL_FRACTIONS),
    ids=lambda fraction: "null-mixed" if fraction is None else f"null-{fraction}",
)
def test_map_in_arrow_randomized_roundtrip(
    spark, tmp_path, seed, batch_size, null_fraction
):
    schema, rows = _generate_case(seed, null_fraction=null_fraction)
    context = (
        f"seed={seed}, batch_size={batch_size}, "
        f"null_fraction={null_fraction}, schema={schema.json()}"
    )
    source_path = str(tmp_path / "input.parquet")
    settings = (
        "spark.comet.enabled",
        "spark.comet.exec.pyarrowUDF.enabled",
        "spark.comet.batchSize",
        "spark.sql.execution.arrow.maxRecordsPerBatch",
    )
    # Comet has defaults for these keys, but Spark RuntimeConfig.get does not
    # expose a Comet default when the key has never been explicitly set.
    previous = {key: spark.conf.get(key, None) for key in settings}
    try:
        # Use Spark to establish the Parquet baseline independently of Comet.
        spark.conf.set("spark.comet.enabled", "false")
        spark.createDataFrame(rows, schema).coalesce(1).write.parquet(source_path)
        baseline = spark.read.parquet(source_path).collect()
        _assert_rows_equal(baseline, rows, f"{context}, mode=parquet-baseline")

        spark.conf.set("spark.comet.enabled", "true")
        # The accelerated path sends Comet source batches directly to Python;
        # Spark's Arrow limit alone would not force multiple input batches.
        spark.conf.set("spark.comet.batchSize", str(batch_size))
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch_size))

        def annotate_batches(iterator):
            for batch_index, batch in enumerate(iterator):
                batch.validate(full=True)
                yield pa.RecordBatch.from_arrays(
                    [
                        *batch.columns,
                        pa.array([batch_index] * batch.num_rows, type=pa.int32()),
                        pa.array([batch.num_rows] * batch.num_rows, type=pa.int32()),
                    ],
                    names=[*batch.schema.names, "batch_index", "batch_size"],
                )

        expected_batch_sizes = [
            min(batch_size, len(rows) - start)
            for start in range(0, len(rows), batch_size)
        ]

        outputs = {}
        for accelerated in (True, False):
            mode = "accelerated" if accelerated else "fallback"
            spark.conf.set(
                "spark.comet.exec.pyarrowUDF.enabled", str(accelerated).lower()
            )
            source = spark.read.parquet(source_path)
            # Batch indices restart per partition; this bounded input is written
            # into one small file so all batches come from one worker iterator.
            assert source.rdd.getNumPartitions() == 1, f"{context}, mode={mode}"
            output_schema = T.StructType(
                [
                    *source.schema.fields,
                    T.StructField("batch_index", T.IntegerType(), nullable=False),
                    T.StructField("batch_size", T.IntegerType(), nullable=False),
                ]
            )
            result = source.mapInArrow(annotate_batches, output_schema)
            plan = result._jdf.queryExecution().executedPlan().toString()
            if accelerated:
                assert "CometMapInBatch" in plan, f"{context}, mode={mode}\n{plan}"
                assert "ColumnarToRow" not in plan, f"{context}, mode={mode}\n{plan}"
            else:
                assert "CometMapInBatch" not in plan, f"{context}, mode={mode}\n{plan}"
                assert "MapInArrow" in plan, f"{context}, mode={mode}\n{plan}"
            annotated_rows = result.collect()
            observed_batches = {}
            for row in annotated_rows:
                observed_batches.setdefault(row["batch_index"], []).append(row)
            assert sorted(observed_batches) == list(range(len(expected_batch_sizes))), (
                f"{context}, mode={mode}, observed_batches={sorted(observed_batches)}"
            )
            for index, expected_size in enumerate(expected_batch_sizes):
                batch_rows = observed_batches[index]
                batch_context = f"{context}, mode={mode}, batch_index={index}"
                assert len(batch_rows) == expected_size, batch_context
                assert {row["batch_size"] for row in batch_rows} == {
                    expected_size
                }, batch_context

            # Compare every original field after checking worker batch metadata.
            # Full row IDs preserve multiplicity and catch losses/duplicates at
            # batch boundaries as well as changes to nested payloads.
            outputs[mode] = [
                {name: row[name] for name in source.columns} for row in annotated_rows
            ]
            _assert_rows_equal(outputs[mode], baseline, f"{context}, mode={mode}")

        _assert_rows_equal(
            outputs["accelerated"], outputs["fallback"], f"{context}, mode=cross-check"
        )
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)

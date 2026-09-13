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
Pytest-driven integration tests for Comet's PyArrow UDF acceleration.

Each test runs against two execution paths:
  - "accelerated": spark.comet.exec.pyarrowUDF.enabled=true
                   (plan should contain CometMapInBatch and no ColumnarToRow)
  - "fallback":    spark.comet.exec.pyarrowUDF.enabled=false
                   (plan should contain vanilla PythonMapInArrow / MapInArrow)

Usage:
    # Build Comet first:
    make

    # Then either let the test discover the jar from spark/target, or pass it
    # explicitly via COMET_JAR:
    export COMET_JAR=$PWD/spark/target/comet-spark-spark3.5_2.12-0.16.0-SNAPSHOT.jar

    pip install pyspark==3.5.9 pyarrow pandas pytest
    pytest -v spark/src/test/resources/pyspark/test_pyarrow_udf.py
"""

import datetime as dt
import os
from collections import Counter
from decimal import Decimal

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession, types as T

from conftest import resolve_comet_jar


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
        .appName("comet-pyarrow-udf-tests")
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


@pytest.fixture
def large_var_types(spark):
    settings = {
        "spark.sql.execution.arrow.useLargeVarTypes": "true",
        "spark.comet.batchSize": "7",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "7",
    }
    previous = {key: spark.conf.get(key, None) for key in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _executed_plan(df) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def _assert_plan_matches_mode(
    plan: str, accelerated: bool, vanilla_node: str = "MapInArrow"
) -> None:
    if accelerated:
        assert "CometMapInBatch" in plan, (
            f"expected CometMapInBatch in accelerated plan, got:\n{plan}"
        )
        assert "ColumnarToRow" not in plan, (
            f"unexpected ColumnarToRow in accelerated plan:\n{plan}"
        )
    else:
        assert "CometMapInBatch" not in plan, (
            f"unexpected CometMapInBatch in fallback plan:\n{plan}"
        )
        assert vanilla_node in plan, (
            f"expected {vanilla_node} in fallback plan, got:\n{plan}"
        )


def test_map_in_arrow_doubles_value(spark, tmp_path, accelerated):
    data = [(i, float(i * 1.5), f"name_{i}") for i in range(100)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(data, ["id", "value", "name"]).write.parquet(src)

    def double_value(iterator):
        for batch in iterator:
            pdf = batch.to_pandas()
            pdf["value"] = pdf["value"] * 2
            yield pa.RecordBatch.from_pandas(pdf)

    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
            T.StructField("name", T.StringType()),
        ]
    )
    result_df = spark.read.parquet(src).mapInArrow(double_value, schema)

    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    rows = result_df.orderBy("id").collect()
    assert len(rows) == len(data)
    for row, original in zip(rows, data):
        assert row["id"] == original[0]
        assert abs(row["value"] - original[1] * 2) < 1e-6
        assert row["name"] == original[2]


# All other tests use the default `vanilla_node="MapInArrow"`. The mapInPandas tests below
# pass `MapInPandas` explicitly. The substring is the same on Spark 3.5 (PythonMapInArrowExec)
# and Spark 4.x (MapInArrowExec) since the latter is a substring of the former.


def test_map_in_arrow_changes_schema(spark, tmp_path, accelerated):
    data = [(i, float(i)) for i in range(50)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(data, ["id", "value"]).write.parquet(src)

    def add_computed_column(iterator):
        for batch in iterator:
            pdf = batch.to_pandas()
            pdf["squared"] = pdf["value"] ** 2
            pdf["label"] = pdf["id"].apply(lambda x: f"item_{x}")
            yield pa.RecordBatch.from_pandas(pdf)

    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
            T.StructField("squared", T.DoubleType()),
            T.StructField("label", T.StringType()),
        ]
    )
    result_df = spark.read.parquet(src).mapInArrow(add_computed_column, schema)

    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    rows = result_df.orderBy("id").collect()
    assert len(rows) == 50
    for i, row in enumerate(rows):
        assert abs(row["squared"] - float(i) ** 2) < 1e-6
        assert row["label"] == f"item_{i}"


def test_map_in_pandas_doubles_value(spark, tmp_path, accelerated):
    data = [(i, float(i * 1.5)) for i in range(100)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(data, ["id", "value"]).write.parquet(src)

    def double_value(iterator):
        for pdf in iterator:
            pdf = pdf.copy()
            pdf["value"] = pdf["value"] * 2
            yield pdf

    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
        ]
    )
    result_df = spark.read.parquet(src).mapInPandas(double_value, schema)

    _assert_plan_matches_mode(
        _executed_plan(result_df), accelerated, vanilla_node="MapInPandas"
    )

    rows = result_df.orderBy("id").collect()
    assert len(rows) == len(data)
    for row, original in zip(rows, data):
        assert row["id"] == original[0]
        assert abs(row["value"] - original[1] * 2) < 1e-6


def test_map_in_pandas_changes_schema(spark, tmp_path, accelerated):
    data = [(i, float(i)) for i in range(50)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(data, ["id", "value"]).write.parquet(src)

    def add_squared(iterator):
        for pdf in iterator:
            pdf = pdf.copy()
            pdf["squared"] = pdf["value"] ** 2
            yield pdf

    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
            T.StructField("squared", T.DoubleType()),
        ]
    )
    result_df = spark.read.parquet(src).mapInPandas(add_squared, schema)

    _assert_plan_matches_mode(
        _executed_plan(result_df), accelerated, vanilla_node="MapInPandas"
    )

    rows = result_df.orderBy("id").collect()
    assert len(rows) == 50
    for i, row in enumerate(rows):
        assert abs(row["squared"] - float(i) ** 2) < 1e-6


def test_map_in_arrow_preserves_nulls(spark, tmp_path, accelerated):
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("name", T.StringType()),
        ]
    )
    rows = [
        (1, "a"),
        (2, None),
        (None, "c"),
        (None, None),
        (5, "e"),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        # Pure Arrow passthrough so nulls survive without a pandas roundtrip
        # (pandas would coerce null longs to NaN floats).
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = {(r["id"], r["name"]) for r in result_df.collect()}
    assert out == set(rows)


def test_map_in_arrow_empty_input(spark, tmp_path, accelerated):
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
        ]
    )
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame([(1, 1.0), (2, 2.0)], schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    # Filter all rows out so the operator sees an empty stream from CometScan.
    result_df = (
        spark.read.parquet(src).where("id < 0").mapInArrow(passthrough, schema_in)
    )
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    assert result_df.count() == 0


def test_map_in_arrow_python_exception_propagates(spark, tmp_path, accelerated):
    schema_in = T.StructType([T.StructField("id", T.LongType())])
    data = [(i,) for i in range(10)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(data, schema_in).write.parquet(src)

    sentinel = "boom-from-pyarrow-udf"

    def boom(iterator):
        for _batch in iterator:
            raise ValueError(sentinel)
        # Unreachable, but mapInArrow requires the callable to be a generator.
        yield  # pragma: no cover

    result_df = spark.read.parquet(src).mapInArrow(boom, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    with pytest.raises(Exception) as exc_info:
        result_df.collect()
    assert sentinel in str(exc_info.value), (
        f"expected sentinel {sentinel!r} in exception, got: {exc_info.value}"
    )


def test_map_in_arrow_decimal_type(spark, tmp_path, accelerated):
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("amount", T.DecimalType(18, 6)),
        ]
    )
    rows = [
        (1, Decimal("123.456789")),
        (2, Decimal("0.000001")),
        (3, Decimal("-99999999.999999")),
        (4, None),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = {(r["id"], r["amount"]) for r in result_df.collect()}
    assert out == set(rows)


@pytest.mark.parametrize(
    "precision,scale",
    [
        (1, 0),
        (9, 0),
        (9, 4),
        (17, 8),
        (18, 0),
        (18, 18),
        (19, 0),
        (28, 14),
        (38, 0),
        (38, 18),
        (38, 38),
    ],
)
def test_map_in_arrow_decimal_precision_sweep(
    spark, tmp_path, accelerated, precision, scale
):
    """
    Arrow's `DecimalVector` is always 16 bytes wide regardless of precision, so there is no
    buffer-width boundary on the direct Arrow path (the 8-byte long-backed form is Spark's
    `UnsafeRow` encoding, a layer this path never sees). This sweep instead guards the
    precision/scale extremes and the 18/19 point where Spark's own decimal handling changes
    representation, keeping the round trip value-exact. Scale extremes: 0, half, max.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("amount", T.DecimalType(precision, scale)),
        ]
    )
    integer_digits = precision - scale
    abs_int = (10**integer_digits - 1) if integer_digits > 0 else 0
    abs_frac = (10**scale - 1) if scale > 0 else 0
    largest = Decimal(f"{abs_int}.{abs_frac:0{scale}d}") if scale else Decimal(abs_int)
    # copy_negate() flips the sign without applying the decimal context. Plain `-largest` would
    # round to the context's default 28 significant digits, turning the 38-digit maximum into
    # 1E38 and overflowing Decimal(38, 0).
    rows = [
        (1, Decimal(0)),
        (2, largest),
        (3, largest.copy_negate()),
        (4, None),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = {(r["id"], r["amount"]) for r in result_df.collect()}
    assert out == set(rows)


@pytest.mark.parametrize("null_fraction", [0.0, 0.01, 0.5, 0.99, 1.0])
def test_map_in_arrow_null_density_sweep(
    spark, tmp_path, accelerated, null_fraction
):
    """
    Sweep validity-buffer density across the corner cases: all-non-null, sparse-null, half-null,
    sparse-non-null, all-null. Catches off-by-one errors in validity packing and mismatches
    between field-node null counts and serialized validity buffers.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.LongType()),
        ]
    )
    n = 256
    rows = [
        (i, None if (i * 9973) % 100 < int(null_fraction * 100) else i * 2)
        for i in range(n)
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = sorted((r["id"], r["value"]) for r in result_df.collect())
    assert out == sorted(rows)


def test_map_in_arrow_multi_batch_per_partition(spark, tmp_path, accelerated):
    """
    Exercise the stream across many rows with a small Spark Arrow batch limit. The accelerated
    path writes complete Comet source batches, so its source-vector turnover is covered separately
    by test_map_in_arrow_nested_source_batches below. Catches stale value counts and changing
    variable-width content on the Spark fallback path.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("s", T.StringType()),
        ]
    )
    n = 4000
    rows = [(i, f"row_{i}" if i % 7 != 0 else None) for i in range(n)]
    src = str(tmp_path / "src.parquet")
    # Single partition; small arrow batch limit forces ~250 batches per partition.
    spark.createDataFrame(rows, schema_in).coalesce(1).write.parquet(src)

    prev_records = spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "16")
    try:

        def passthrough(iterator):
            for batch in iterator:
                yield batch

        result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
        _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

        out = sorted((r["id"], r["s"]) for r in result_df.collect())
        assert out == sorted(rows)
    finally:
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", prev_records)


def test_map_in_arrow_nested_source_batches(spark, tmp_path, accelerated):
    """Serialize changing nested/null source vectors directly across actual worker batches."""
    item_type = T.StructType(
        [
            T.StructField("label", T.StringType()),
            T.StructField("score", T.IntegerType()),
        ]
    )
    payload_type = T.StructType(
        [
            T.StructField("items", T.ArrayType(item_type, containsNull=True)),
            T.StructField(
                "attrs",
                T.MapType(T.StringType(), T.IntegerType(), valueContainsNull=True),
            ),
        ]
    )
    input_schema = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("payload", payload_type),
        ]
    )
    output_schema = T.StructType(
        [
            *input_schema.fields,
            T.StructField("batch_index", T.IntegerType(), nullable=False),
            T.StructField("batch_size", T.IntegerType(), nullable=False),
        ]
    )

    rows = []
    for index in range(37):
        if index % 11 == 0:
            payload = None
        else:
            if index % 5 == 0:
                items = None
            else:
                items = [
                    None
                    if (index + offset) % 6 == 0
                    else {
                        "label": None
                        if (index + offset) % 4 == 0
                        else f"item_{index}_{'x' * offset}",
                        "score": None
                        if (index + offset) % 3 == 0
                        else index * 10 + offset,
                    }
                    for offset in range(index % 4)
                ]

            if index % 7 == 0:
                attrs = None
            elif index % 6 == 0:
                attrs = {}
            else:
                attrs = {
                    f"key_{index % 3}": None if index % 4 == 0 else index,
                    "marker": index * 10,
                }
            payload = {"items": items, "attrs": attrs}
        rows.append((index, payload))

    src = str(tmp_path / "nested_source_batches.parquet")
    spark.createDataFrame(rows, input_schema).coalesce(1).write.parquet(src)

    batch_size = 7
    previous_comet_batch_size = spark.conf.get("spark.comet.batchSize", "8192")
    previous_arrow_batch_size = spark.conf.get(
        "spark.sql.execution.arrow.maxRecordsPerBatch"
    )
    spark.conf.set("spark.comet.batchSize", str(batch_size))
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch_size))
    try:

        def annotate_batches(iterator):
            for batch_index, batch in enumerate(iterator):
                assert batch.schema.names == ["id", "payload"]
                yield pa.RecordBatch.from_arrays(
                    [
                        *batch.columns,
                        pa.array([batch_index] * batch.num_rows, type=pa.int32()),
                        pa.array([batch.num_rows] * batch.num_rows, type=pa.int32()),
                    ],
                    names=[*batch.schema.names, "batch_index", "batch_size"],
                )

        result_df = spark.read.parquet(src).mapInArrow(annotate_batches, output_schema)
        _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

        output = result_df.collect()
        assert len(output) == len(rows)
        expected_payloads = dict(rows)
        observed_batches = {}
        for row in output:
            payload = row["payload"]
            actual_payload = None if payload is None else payload.asDict(recursive=True)
            assert actual_payload == expected_payloads[row["id"]]
            observed_batches.setdefault(row["batch_index"], []).append(row)

        assert sorted(observed_batches) == list(range(6))
        expected_batch_sizes = [batch_size] * 5 + [2]
        observed_batch_sizes = [len(observed_batches[index]) for index in range(6)]
        assert observed_batch_sizes == expected_batch_sizes
        for batch_rows in observed_batches.values():
            assert {row["batch_size"] for row in batch_rows} == {len(batch_rows)}
    finally:
        spark.conf.set("spark.comet.batchSize", previous_comet_batch_size)
        spark.conf.set(
            "spark.sql.execution.arrow.maxRecordsPerBatch", previous_arrow_batch_size
        )


@pytest.mark.parametrize(
    "wrapper,rename_field,api",
    [
        (wrapper, rename_field, api)
        for wrapper, rename_field in [
            ("{}", False),
            ("array({})", False),
            ("map(1,{})", False),
            ("map({},1)", False),
            ("map(1,array({}))", False),
            ("{}", True),
            ("array({})", True),
            ("map(1,{})", True),
        ]
        for api in ["mapInArrow", "mapInPandas"]
        # Spark's pandas conversion cannot use dict-valued struct keys in a map.
        if api == "mapInArrow" or wrapper != "map({},1)"
    ],
)
def test_map_in_batch_union_with_compatible_nested_fields(
    spark, tmp_path, accelerated, api, wrapper, rename_field
):
    """Union batches can vary in names/nullability without changing their buffer layout."""
    rows = [(0, None), (1, 2), (2, 3)]
    src = str(tmp_path / "union_fields.parquet")
    spark.createDataFrame(rows, "id int, value int").coalesce(1).write.parquet(src)
    source = spark.read.parquet(src)
    left_value = "named_struct('id', id, 'value', 1)"
    right_name = "renamed_value" if rename_field else "value"
    right_value = f"named_struct('id', id, '{right_name}', value)"
    if wrapper == "map(1,{})":
        right_value = f"IF(value IS NULL, NULL, {right_value})"
    left = source.selectExpr(f"{wrapper.format(left_value)} AS payload")
    right = source.selectExpr(f"{wrapper.format(right_value)} AS payload")
    combined = left.union(right).coalesce(1)
    expected = Counter(map(repr, combined.collect()))

    def passthrough(iterator):
        for batch in iterator:
            if api == "mapInArrow":
                data_type = batch.schema.field("payload").type
                if pa.types.is_map(data_type):
                    assert not data_type.field(0).nullable  # entries
                    assert not data_type.key_field.nullable
                    nested = data_type.key_type
                    if not pa.types.is_struct(nested):
                        nested = data_type.item_type
                        if pa.types.is_struct(nested):
                            assert data_type.item_field.nullable
                    if pa.types.is_list(nested):
                        nested = nested.value_type
                    assert nested.field("value").nullable
            yield batch

    result = getattr(combined, api)(passthrough, combined.schema)
    plan = _executed_plan(result)
    _assert_plan_matches_mode(
        plan,
        accelerated,
        vanilla_node="MapInArrow" if api == "mapInArrow" else "MapInPandas",
    )
    if accelerated:
        assert "CometUnion" in plan
    assert Counter(map(repr, result.collect())) == expected


@pytest.mark.parametrize("api", ["mapInArrow", "mapInPandas"])
@pytest.mark.parametrize("timezone", ["UTC", "Etc/UTC"])
@pytest.mark.parametrize(
    "truncated_first", [False, True], ids=["scan-first", "truncation-first"]
)
def test_map_in_batch_union_with_utc_timezone_alias(
    spark, tmp_path, accelerated, api, timezone, truncated_first
):
    """Native scans use UTC, while date_trunc preserves the equivalent Etc/UTC alias."""
    previous_timezone = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", timezone)
    try:
        schema = T.StructType(
            [
                T.StructField("id", T.LongType()),
                T.StructField("ts", T.TimestampType()),
            ]
        )
        rows = [
            (0, None),
            (1, dt.datetime(2024, 1, 2, 3, 4, 5, 123456)),
            (2, dt.datetime(2024, 1, 2, 23, 59, 59, 999999)),
            (3, dt.datetime(2024, 1, 3)),
        ]
        src = str(tmp_path / "union_timestamp_timezones.parquet")
        spark.createDataFrame(rows, schema).coalesce(1).write.parquet(src)
        source = spark.read.parquet(src)
        truncated = source.selectExpr("id", "date_trunc('day', ts) AS ts")
        left, right = (truncated, source) if truncated_first else (source, truncated)
        # Put both source schemas in one worker's input stream, in either order.
        combined = left.union(right).coalesce(1)
        expected = Counter(tuple(row) for row in combined.collect())
        assert sum(expected.values()) == 2 * len(rows)
        assert expected[(0, None)] == 2

        def passthrough(iterator):
            yield from iterator

        result = getattr(combined, api)(passthrough, combined.schema)
        plan = _executed_plan(result)
        _assert_plan_matches_mode(
            plan,
            accelerated,
            vanilla_node="MapInArrow" if api == "mapInArrow" else "MapInPandas",
        )
        assert "CometUnion" in plan
        assert "CometCoalesce" in plan
        assert "CometProject" in plan
        assert Counter(tuple(row) for row in result.collect()) == expected
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_timezone)


@pytest.mark.parametrize("api", ["mapInArrow", "mapInPandas"])
def test_map_in_batch_union_with_different_field_metadata(
    spark, tmp_path, accelerated, api
):
    """Parquet field IDs describe source columns, not the outgoing IPC buffer layout."""
    sources = []
    for field_id in (1, 2):
        nested = T.StructType(
            [T.StructField("value", T.IntegerType(), True, {"parquet.field.id": field_id})]
        )
        schema = T.StructType([T.StructField("payload", nested)])
        path = str(tmp_path / f"field_id_{field_id}.parquet")
        spark.createDataFrame([((10,),), ((None,),)], schema).coalesce(1).write.parquet(path)
        sources.append(spark.read.parquet(path))
    combined = sources[0].union(sources[1]).coalesce(1)

    def passthrough(iterator):
        yield from iterator

    result = getattr(combined, api)(passthrough, combined.schema)
    plan = _executed_plan(result)
    _assert_plan_matches_mode(
        plan,
        accelerated,
        vanilla_node="MapInArrow" if api == "mapInArrow" else "MapInPandas",
    )
    if accelerated:
        assert "CometUnion" in plan
    assert Counter(row.payload.value for row in result.collect()) == Counter({10: 2, None: 2})


def test_map_in_arrow_wide_schema(spark, tmp_path, accelerated):
    """
    50-column mixed-type schema. Direct serialization must preserve every source vector and its
    independent null bitmap when building the IPC field-node and buffer lists.
    """
    fields = [T.StructField("id", T.LongType())]
    for i in range(15):
        fields.append(T.StructField(f"i{i}", T.IntegerType()))
    for i in range(15):
        fields.append(T.StructField(f"d{i}", T.DoubleType()))
    for i in range(15):
        fields.append(T.StructField(f"s{i}", T.StringType()))
    for i in range(4):
        fields.append(T.StructField(f"b{i}", T.BooleanType()))
    assert len(fields) == 50
    schema_in = T.StructType(fields)

    rows = []
    for i in range(60):
        row = [i]
        row += [i + k if k % 3 != 0 else None for k in range(15)]
        row += [float(i + k) * 0.5 if k % 4 != 0 else None for k in range(15)]
        row += [f"s{i}_{k}" if k % 5 != 0 else None for k in range(15)]
        row += [bool((i + k) % 2) for k in range(4)]
        rows.append(tuple(row))

    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = sorted(tuple(r[name] for name in schema_in.names) for r in result_df.collect())
    assert out == sorted(rows)


def test_map_in_arrow_zero_row_batch_in_stream(spark, tmp_path, accelerated):
    """
    A non-empty stream that contains a 0-row batch mid-stream. The existing empty-input test
    filters everything out so the operator sees zero batches; this one keeps later batches so
    the writer must handle a 0-row batch and continue. setValueCount(0) + validity buffer
    sizing are the candidates that can break here.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.LongType()),
        ]
    )
    rows = [(i, i * 3) for i in range(50)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).coalesce(1).write.parquet(src)

    def emit_with_empty(iterator):
        for batch in iterator:
            # Yield an empty record batch first, then the real one.
            yield batch.slice(0, 0)
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(emit_with_empty, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = sorted((r["id"], r["value"]) for r in result_df.collect())
    assert out == sorted(rows)


def test_map_in_arrow_transforming_array(spark, tmp_path, accelerated):
    """
    Mutating UDF over a complex type: reverse each array. Catches symmetric encode/decode
    mistakes that a passthrough UDF would invert and hide.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("nums", T.ArrayType(T.IntegerType())),
        ]
    )
    rows = [
        (1, [1, 2, 3, 4]),
        (2, [None, 5, None]),
        (3, []),
        (4, None),
        (5, [42]),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def reverse_arrays(iterator):
        for batch in iterator:
            pdf = batch.to_pandas()
            pdf["nums"] = pdf["nums"].apply(
                lambda lst: list(reversed(lst)) if lst is not None else None
            )
            # Pin the output to the incoming Arrow schema. Without it,
            # from_pandas infers list<int64> from the Python-int lists, mismatching
            # the declared array<int> (list<int32>) output and tripping Spark's
            # int32 projection over the result.
            yield pa.RecordBatch.from_pandas(pdf, schema=batch.schema)

    result_df = spark.read.parquet(src).mapInArrow(reverse_arrays, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    def _norm(row):
        nums = row["nums"]
        return (row["id"], None if nums is None else tuple(nums))

    out = {_norm(r) for r in result_df.collect()}
    expected = set()
    for id_, nums in rows:
        rev = None if nums is None else tuple(reversed(nums))
        expected.add((id_, rev))
    assert out == expected


def test_map_in_arrow_date_and_timestamp(spark, tmp_path, accelerated):
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("d", T.DateType()),
            T.StructField("ts", T.TimestampType()),
        ]
    )
    rows = [
        (1, dt.date(2024, 1, 1), dt.datetime(2024, 1, 1, 12, 30, 45)),
        (2, dt.date(1999, 12, 31), dt.datetime(2000, 6, 15, 0, 0, 0)),
        (3, None, None),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = {(r["id"], r["d"], r["ts"]) for r in result_df.collect()}
    assert out == set(rows)


def test_map_in_arrow_array_and_struct(spark, tmp_path, accelerated):
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("nums", T.ArrayType(T.IntegerType())),
            T.StructField(
                "addr",
                T.StructType(
                    [
                        T.StructField("city", T.StringType()),
                        T.StructField("zip", T.IntegerType()),
                    ]
                ),
            ),
        ]
    )
    rows = [
        (1, [1, 2, 3], ("Berlin", 10115)),
        (2, [], ("NYC", 10001)),
        (3, None, None),
        (4, [None, 5], ("Tokyo", None)),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    def _normalize(row):
        nums = tuple(row["nums"]) if row["nums"] is not None else None
        addr = row["addr"]
        addr_tuple = (addr["city"], addr["zip"]) if addr is not None else None
        return (row["id"], nums, addr_tuple)

    out = {_normalize(r) for r in result_df.collect()}
    expected = {
        (r[0], tuple(r[1]) if r[1] is not None else None, r[2]) for r in rows
    }
    assert out == expected


def test_map_in_arrow_numeric_scalars(spark, tmp_path, accelerated):
    """
    Covers every fixed-width primitive Comet's scan supports beyond the long/double/int already
    exercised by other tests: boolean, byte, short, float. Each has a distinct buffer size, and
    the validity bit handling is independent per directly serialized source vector.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("b", T.BooleanType()),
            T.StructField("tiny", T.ByteType()),
            T.StructField("small", T.ShortType()),
            T.StructField("flt", T.FloatType()),
        ]
    )
    rows = [
        (1, True, 1, 1000, 1.5),
        (2, False, -128, -32768, -3.25),
        (3, True, 127, 32767, float("inf")),
        (4, None, None, None, None),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    # ShortType (the `small` column) forces the Comet scan to fall back to vanilla Spark by
    # default: Parquet UINT_8 maps to ShortType and Comet cannot distinguish it from signed
    # INT16 (spark.comet.scan.unsignedSmallIntSafetyCheck). Without a Comet scan there is no
    # columnar producer for the UDF to consume, so the rewrite cannot fire. This data is signed,
    # so allow native execution to exercise the accelerated path.
    prev_uint_check = spark.conf.get(
        "spark.comet.scan.unsignedSmallIntSafetyCheck", "true"
    )
    spark.conf.set("spark.comet.scan.unsignedSmallIntSafetyCheck", "false")
    try:
        result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
        _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

        out = {
            (r["id"], r["b"], r["tiny"], r["small"], r["flt"])
            for r in result_df.collect()
        }
        assert out == set(rows)
    finally:
        spark.conf.set("spark.comet.scan.unsignedSmallIntSafetyCheck", prev_uint_check)


def test_map_in_arrow_binary_type(spark, tmp_path, accelerated):
    """
    BinaryType is the BaseVariableWidthVector path with non-string content. StringType
    already exercises that path for utf-8 data; binary covers the case where the data
    buffer can hold arbitrary bytes (including null bytes mid-string).
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("payload", T.BinaryType()),
        ]
    )
    rows = [
        (1, b"\x00\x01\x02\x03"),
        (2, b""),
        (3, b"\xff" * 64),
        (4, None),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = {(r["id"], bytes(r["payload"]) if r["payload"] is not None else None)
           for r in result_df.collect()}
    expected = set(rows)
    assert out == expected


def test_map_in_arrow_timestamp_ntz(spark, tmp_path, accelerated):
    """
    TimestampNTZType is a separate Arrow type from TimestampType (no timezone) and goes
    through a different ArrowType.Timestamp(..., tz=None) on the wire.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("ts_ntz", T.TimestampNTZType()),
        ]
    )
    rows = [
        (1, dt.datetime(2024, 1, 1, 12, 30, 45)),
        (2, dt.datetime(1970, 1, 1, 0, 0, 0)),
        (3, None),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = {(r["id"], r["ts_ntz"]) for r in result_df.collect()}
    assert out == set(rows)


def test_map_in_arrow_map_type(spark, tmp_path, accelerated):
    """
    MapType is encoded in Arrow as a List<Struct<key, value>> with extra metadata. The
    buffer layout (offsets + struct child + key/value children) is distinct from a plain
    list, and CometMapVector is a separate vector class from CometListVector. Without this test,
    direct recursive serialization of map-typed source vectors is unexercised.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField(
                "attrs", T.MapType(T.StringType(), T.IntegerType(), valueContainsNull=True)
            ),
        ]
    )
    rows = [
        (1, {"a": 1, "b": 2}),
        (2, {}),
        (3, None),
        (4, {"only": None}),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    def _normalize(row):
        attrs = row["attrs"]
        attrs_norm = (
            tuple(sorted(attrs.items(), key=lambda kv: kv[0]))
            if attrs is not None
            else None
        )
        return (row["id"], attrs_norm)

    out = {_normalize(r) for r in result_df.collect()}
    expected = {
        (
            r[0],
            tuple(sorted(r[1].items(), key=lambda kv: kv[0])) if r[1] is not None else None,
        )
        for r in rows
    }
    assert out == expected


def test_map_in_arrow_deeply_nested(spark, tmp_path, accelerated):
    """
    Exercises direct source-vector serialization at depth > 1, in every nesting combination:
    array-of-array, array-of-struct, struct-of-array, struct-of-struct. Single-level nesting is
    covered by test_map_in_arrow_array_and_struct; the bug surface here is that Arrow field
    nodes and buffers must remain in the expected depth-first order at every level.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("matrix", T.ArrayType(T.ArrayType(T.IntegerType()))),
            T.StructField(
                "people",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("name", T.StringType()),
                            T.StructField("age", T.IntegerType()),
                        ]
                    )
                ),
            ),
            T.StructField(
                "config",
                T.StructType(
                    [
                        T.StructField("flags", T.ArrayType(T.StringType())),
                        T.StructField(
                            "limits",
                            T.StructType(
                                [
                                    T.StructField("min", T.IntegerType()),
                                    T.StructField("max", T.IntegerType()),
                                ]
                            ),
                        ),
                    ]
                ),
            ),
        ]
    )
    rows = [
        (
            1,
            [[1, 2], [3, 4, 5]],
            [("alice", 30), ("bob", 25)],
            (["x", "y"], (0, 100)),
        ),
        (
            2,
            [[], [None, 7]],
            [("solo", None)],
            ([], (None, None)),
        ),
        (3, None, None, None),
        (4, [None, [9]], [None, ("ghost", 0)], (None, None)),
    ]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = spark.read.parquet(src).mapInArrow(passthrough, schema_in)
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    def _norm_array(a):
        return tuple(a) if a is not None else None

    def _norm_matrix(m):
        return tuple(_norm_array(inner) for inner in m) if m is not None else None

    def _norm_people(p):
        if p is None:
            return None
        return tuple(
            (item["name"], item["age"]) if item is not None else None for item in p
        )

    def _norm_config(c):
        if c is None:
            return None
        flags = _norm_array(c["flags"])
        limits = c["limits"]
        limits_norm = (limits["min"], limits["max"]) if limits is not None else None
        return (flags, limits_norm)

    def _norm_row(r):
        return (
            r["id"],
            _norm_matrix(r["matrix"]),
            _norm_people(r["people"]),
            _norm_config(r["config"]),
        )

    def _norm_input_people(p):
        if p is None:
            return None
        return tuple(item if item is not None else None for item in p)

    def _norm_input_config(c):
        if c is None:
            return None
        flags, limits = c
        return (_norm_array(flags), limits)

    out = {_norm_row(r) for r in result_df.collect()}
    expected = {
        (
            r[0],
            _norm_matrix(r[1]),
            _norm_input_people(r[2]),
            _norm_input_config(r[3]),
        )
        for r in rows
    }
    assert out == expected


@pytest.mark.parametrize("api", ["mapInArrow", "mapInPandas"])
@pytest.mark.parametrize("union_input", [False, True])
def test_large_var_types_nested_batches_and_chained_udfs(
    spark, tmp_path, accelerated, large_var_types, api, union_input
):
    """Widen native offsets, then reuse already-large output in the next Python runner."""
    schema = (
        "id int, text string, data binary, nested struct<text:string,data:binary>, "
        "texts array<string>, binaries array<binary>, "
        "mapping map<string,struct<text:string,data:binary>>"
    )
    rows = []
    for index in range(37):
        text = None if index % 4 == 0 else f"λ中文-{index}"
        data = None if index % 5 == 0 else bytearray([0, index, 255])
        rows.append(
            (
                index,
                text,
                data,
                None if index % 3 == 0 else (text, data),
                [text, "", None],
                [data, bytearray(), None],
                {f"key-{index}": (text, data), "missing": None},
            )
        )
    path = str(tmp_path / "large_types.parquet")
    spark.createDataFrame(rows, schema).coalesce(1).write.parquet(path)
    source = spark.read.parquet(path)
    if union_input:
        other = source.selectExpr(
            "id + 37 AS id",
            "text",
            "data",
            "named_struct('renamed_text', nested.text, 'renamed_data', nested.data) AS nested",
            "texts",
            "binaries",
            "mapping",
        )
        source = source.union(other).coalesce(1)
    expected = sorted(source.collect(), key=lambda row: row.id)

    def check_arrow_batch(batch):
        assert batch.num_rows <= 7
        fields = batch.schema
        assert pa.types.is_large_string(fields.field("text").type)
        assert pa.types.is_large_binary(fields.field("data").type)
        nested = fields.field("nested").type
        assert pa.types.is_large_string(nested.field("text").type)
        assert pa.types.is_large_binary(nested.field("data").type)
        assert pa.types.is_large_string(fields.field("texts").type.value_type)
        assert pa.types.is_large_binary(fields.field("binaries").type.value_type)
        mapping = fields.field("mapping").type
        assert pa.types.is_large_string(mapping.key_type)
        assert not mapping.key_field.nullable
        assert pa.types.is_large_string(mapping.item_type.field("text").type)
        assert pa.types.is_large_binary(mapping.item_type.field("data").type)

    def first(iterator):
        for batch in iterator:
            if api == "mapInArrow":
                check_arrow_batch(batch)
                yield batch.slice(0, 0)
            else:
                yield batch.iloc[:0]
            yield batch

    def second(iterator):
        for batch in iterator:
            check_arrow_batch(batch)
            yield batch

    result = getattr(source, api)(first, source.schema).mapInArrow(
        second, source.schema
    )
    plan = _executed_plan(result)
    _assert_plan_matches_mode(plan, accelerated)
    if accelerated:
        assert plan.count("CometMapInBatch") == 2
        if union_input:
            assert "CometUnion" in plan
    assert sorted(result.collect(), key=lambda row: row.id) == expected


@pytest.mark.parametrize("api", ["mapInArrow", "mapInPandas"])
def test_large_var_types_with_no_input_batches(
    spark, tmp_path, accelerated, large_var_types, api
):
    path = str(tmp_path / "empty_large_types.parquet")
    spark.createDataFrame(
        [(1, "a", bytearray(b"x"))], "id int, text string, data binary"
    ).coalesce(1).write.parquet(path)
    source = spark.read.parquet(path).where("id < 0")

    def emit_without_input(iterator):
        assert (
            sum(
                batch.num_rows if api == "mapInArrow" else len(batch)
                for batch in iterator
            )
            == 0
        )
        if api == "mapInArrow":
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array(["empty"], type=pa.large_string()),
                    pa.array([b""], type=pa.large_binary()),
                ],
                names=["text", "data"],
            )
        else:
            import pandas as pd

            yield pd.DataFrame(
                {"text": pd.Series(["empty"], dtype=object), "data": [b""]}
            )

    result = getattr(source, api)(emit_without_input, "text string, data binary")
    _assert_plan_matches_mode(
        _executed_plan(result),
        accelerated,
        vanilla_node="MapInArrow" if api == "mapInArrow" else "MapInPandas",
    )
    assert [(row.text, row.data) for row in result.collect()] == [
        ("empty", bytearray())
    ]


def test_map_in_arrow_after_shuffle(spark, tmp_path, accelerated):
    """
    Verifies correctness when a shuffle sits between the Comet scan and the
    Python UDF. Without `spark.shuffle.manager` configured at session startup
    the shuffle stays a vanilla `Exchange`, which is not columnar, so the
    optimization does not fire across it today. This test does not assert on
    the plan; it only ensures the path produces correct results in both modes
    so a future change that wires Comet shuffle into the optimization does
    not silently break correctness.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
        ]
    )
    rows = [(i, float(i)) for i in range(50)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = (
        spark.read.parquet(src)
        .repartition(4, "id")
        .mapInArrow(passthrough, schema_in)
    )

    out = sorted((r["id"], r["value"]) for r in result_df.collect())
    assert out == sorted(rows)


def test_chained_map_in_arrow(spark, tmp_path, accelerated):
    """
    `df.mapInArrow(udf1).mapInArrow(udf2)` stacks two operators. With the rewrite
    enabled both become `CometMapInBatchExec`, so the inner one's output feeds
    the outer one's input. The outer operator's input path expects vectors of
    `CometDecodedVector` type: if the inner's output is plain `ArrowColumnVector`
    the outer throws `ClassCastException` on the first batch.
    """
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
        ]
    )
    rows = [(i, float(i)) for i in range(50)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema).write.parquet(src)

    def add_one(iterator):
        for batch in iterator:
            pdf = batch.to_pandas()
            pdf["value"] = pdf["value"] + 1.0
            yield pa.RecordBatch.from_pandas(pdf)

    def double_value(iterator):
        for batch in iterator:
            pdf = batch.to_pandas()
            pdf["value"] = pdf["value"] * 2.0
            yield pa.RecordBatch.from_pandas(pdf)

    result_df = (
        spark.read.parquet(src)
        .mapInArrow(add_one, schema)
        .mapInArrow(double_value, schema)
    )

    if accelerated:
        plan = _executed_plan(result_df)
        assert plan.count("CometMapInBatch") >= 2, (
            f"expected two CometMapInBatch operators in accelerated plan, got:\n{plan}"
        )

    out = sorted((r["id"], r["value"]) for r in result_df.collect())
    expected = sorted((i, (float(i) + 1.0) * 2.0) for i in range(50))
    assert out == expected


def test_filter_on_map_in_arrow_output(spark, tmp_path, accelerated):
    """
    A filter on the UDF output column is a downstream Comet operator (when Comet's
    native filter applies) reading from `CometMapInBatchExec`'s output. If the
    output were plain `ArrowColumnVector`, NativeUtil.exportBatch's case match
    would fall to the `case c =>` arm and throw SparkException.
    """
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.LongType()),
        ]
    )
    rows = [(i, i * 2) for i in range(100)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = (
        spark.read.parquet(src).mapInArrow(passthrough, schema).filter("value > 50")
    )

    out = sorted((r["id"], r["value"]) for r in result_df.collect())
    expected = sorted((i, i * 2) for i in range(100) if i * 2 > 50)
    assert out == expected


def test_aggregate_on_map_in_arrow_output(spark, tmp_path, accelerated):
    """
    `mapInArrow(...).groupBy(...).agg(...)` puts an aggregate over the UDF output.
    The aggregate is a Comet operator and reads from `CometMapInBatchExec`'s
    output via NativeUtil.exportBatch when promoted to the native pipeline. If
    the output were ArrowColumnVector, exportBatch would throw on every batch.
    """
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("grp", T.LongType()),
            T.StructField("value", T.LongType()),
        ]
    )
    rows = [(i, i % 5, i) for i in range(100)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema).write.parquet(src)

    def passthrough(iterator):
        for batch in iterator:
            yield batch

    result_df = (
        spark.read.parquet(src)
        .mapInArrow(passthrough, schema)
        .groupBy("grp")
        .agg({"value": "sum"})
    )

    out = {r["grp"]: r["sum(value)"] for r in result_df.collect()}
    expected = {}
    for i in range(100):
        expected[i % 5] = expected.get(i % 5, 0) + i
    assert out == expected


def test_map_in_arrow_barrier_mode(spark, tmp_path, accelerated):
    """
    `mapInArrow(..., barrier=True)` runs the stage in barrier execution mode
    (gang scheduling, all-or-nothing failure semantics, BarrierTaskContext
    available inside the UDF). The optimization captures isBarrier in the
    operator constructor and must propagate it through to RDD.barrier();
    otherwise the runtime context the UDF sees changes when the optimization
    fires and any code calling BarrierTaskContext APIs breaks.
    """
    schema_in = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("value", T.DoubleType()),
        ]
    )
    rows = [(i, float(i)) for i in range(20)]
    src = str(tmp_path / "src.parquet")
    spark.createDataFrame(rows, schema_in).write.parquet(src)

    def assert_barrier_context(iterator):
        from pyspark import BarrierTaskContext

        # Will raise if the task is not running inside a barrier stage.
        BarrierTaskContext.get()
        for batch in iterator:
            yield batch

    result_df = (
        spark.read.parquet(src).mapInArrow(
            assert_barrier_context, schema_in, barrier=True
        )
    )
    _assert_plan_matches_mode(_executed_plan(result_df), accelerated)

    out = sorted((r["id"], r["value"]) for r in result_df.collect())
    assert out == sorted(rows)

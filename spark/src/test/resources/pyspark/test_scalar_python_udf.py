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
Pytest-driven integration tests for Comet's scalar Python UDF acceleration
(`ArrowEvalPythonExec` -> `CometArrowEvalPythonExec`).

Spark routes three user-facing UDF families through that one operator, and each
is exercised here:
  - a plain `udf()` with `spark.sql.execution.pythonUDF.arrow.enabled=true`
    (eval type SQL_ARROW_BATCHED_UDF)
  - `@pandas_udf` scalar (SQL_SCALAR_PANDAS_UDF)
  - `@arrow_udf` scalar, Spark 4.1+ (SQL_SCALAR_ARROW_UDF)

Each test runs against two execution paths:
  - "accelerated": spark.comet.exec.pyarrowUDF.enabled=true
                   (plan should contain CometArrowEvalPython)
  - "fallback":    spark.comet.exec.pyarrowUDF.enabled=false
                   (plan should contain vanilla ArrowEvalPython)

Usage:
    # Build Comet first:
    make

    # Then either let the test discover the jar from spark/target, or pass it
    # explicitly via COMET_JAR:
    export COMET_JAR=$PWD/spark/target/comet-spark-spark4.1_2.13-0.16.0-SNAPSHOT.jar

    pip install pyspark==4.1.3 pyarrow pandas pytest
    pytest -v spark/src/test/resources/pyspark/test_scalar_python_udf.py
"""

import re
from decimal import Decimal

import pyspark
import pytest
from pyspark.sql import functions as F, types as T

from conftest import executed_plan

SPARK_VERSION = tuple(int(p) for p in pyspark.__version__.split(".")[:2])

# `@arrow_udf` (SQL_SCALAR_ARROW_UDF) is new in Spark 4.1.
requires_arrow_udf = pytest.mark.skipif(
    SPARK_VERSION < (4, 1), reason="@arrow_udf requires Spark 4.1+"
)


@pytest.fixture
def arrow_udf_conf(spark):
    """Route a plain `udf()` to ArrowEvalPythonExec rather than BatchEvalPythonExec.

    Comet deliberately does not flip this on the user's behalf: it changes
    Spark's own type coercion and error semantics, not just the transport.
    """
    previous = spark.conf.get("spark.sql.execution.pythonUDF.arrow.enabled")
    spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")
    try:
        yield
    finally:
        spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", previous)


def _without_comet_transitions(plan: str) -> str:
    """Drop Comet's own columnar-to-row nodes, whose names contain 'ColumnarToRow'."""
    return re.sub(r"Comet(Native)?ColumnarToRow", "", plan)


def _assert_plan_matches_mode(plan: str, accelerated: bool) -> None:
    if accelerated:
        assert "CometArrowEvalPython" in plan, (
            f"expected CometArrowEvalPython in accelerated plan, got:\n{plan}"
        )
        assert "ColumnarToRow" not in _without_comet_transitions(plan), (
            f"unexpected vanilla ColumnarToRow in accelerated plan:\n{plan}"
        )
    else:
        assert "CometArrowEvalPython" not in plan, (
            f"unexpected CometArrowEvalPython in fallback plan:\n{plan}"
        )
        assert "ArrowEvalPython" in plan, (
            f"expected ArrowEvalPython in fallback plan, got:\n{plan}"
        )


def _assert_falls_back(plan: str) -> None:
    assert "CometArrowEvalPython" not in plan, (
        f"expected the operator to fall back, got:\n{plan}"
    )
    assert "ArrowEvalPython" in plan, f"expected ArrowEvalPython in plan:\n{plan}"


def _write(spark, tmp_path, rows, columns, name="src.parquet"):
    path = str(tmp_path / name)
    spark.createDataFrame(rows, columns).write.parquet(path)
    return spark.read.parquet(path)


# ---------------------------------------------------------------------------
# Arrow-optimized `udf()` (SQL_ARROW_BATCHED_UDF)
# ---------------------------------------------------------------------------


def test_arrow_batched_udf_doubles_value(
    spark, tmp_path, accelerated, arrow_udf_conf
):
    data = [(i, float(i * 1.5)) for i in range(100)]
    df = _write(spark, tmp_path, data, ["id", "value"])

    doubled = F.udf(lambda v: v * 2, T.DoubleType())
    result = df.withColumn("doubled", doubled("value"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("id").collect()
    assert len(rows) == len(data)
    for row, (expected_id, expected_value) in zip(rows, data):
        assert row["id"] == expected_id
        assert row["value"] == pytest.approx(expected_value)
        assert row["doubled"] == pytest.approx(expected_value * 2)


def test_arrow_batched_udf_preserves_all_child_columns(
    spark, tmp_path, accelerated, arrow_udf_conf
):
    """The pass-through columns are copied out of the input batch, so a wide
    child with mixed types is the interesting case."""
    data = [
        (i, float(i), f"name_{i}", i % 2 == 0, Decimal(f"{i}.25"))
        for i in range(50)
    ]
    df = _write(spark, tmp_path, data, ["id", "d", "name", "flag", "amount"])

    upper = F.udf(lambda s: s.upper(), T.StringType())
    result = df.withColumn("upper", upper("name"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("id").collect()
    assert len(rows) == len(data)
    for row, original in zip(rows, data):
        assert row["id"] == original[0]
        assert row["d"] == pytest.approx(original[1])
        assert row["name"] == original[2]
        assert row["flag"] == original[3]
        assert row["amount"] == original[4]
        assert row["upper"] == original[2].upper()


def test_arrow_batched_udf_handles_nulls(
    spark, tmp_path, accelerated, arrow_udf_conf
):
    data = [(i, None if i % 3 == 0 else i * 2) for i in range(30)]
    df = _write(spark, tmp_path, data, ["id", "value"])

    def add_ten(v):
        return None if v is None else v + 10

    result = df.withColumn("plus", F.udf(add_ten, T.LongType())("value"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("id").collect()
    for row, (expected_id, value) in zip(rows, data):
        assert row["id"] == expected_id
        assert row["value"] == value
        assert row["plus"] == (None if value is None else value + 10)


def test_arrow_batched_udf_multiple_arguments(
    spark, tmp_path, accelerated, arrow_udf_conf
):
    data = [(i, i * 2, i * 3) for i in range(40)]
    df = _write(spark, tmp_path, data, ["a", "b", "c"])

    combine = F.udf(lambda x, y: x + y, T.LongType())
    result = df.withColumn("sum", combine("a", "c"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("a").collect()
    for row, (a, b, c) in zip(rows, data):
        assert (row["a"], row["b"], row["c"]) == (a, b, c)
        assert row["sum"] == a + c


def test_arrow_batched_udf_repeated_argument_is_deduplicated(
    spark, tmp_path, accelerated, arrow_udf_conf
):
    """`f(a, a)` sends column `a` once; both argument offsets point at it."""
    data = [(i,) for i in range(20)]
    df = _write(spark, tmp_path, data, ["a"])

    square = F.udf(lambda x, y: x * y, T.LongType())
    result = df.withColumn("sq", square("a", "a"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("a").collect()
    for row, (a,) in zip(rows, data):
        assert row["sq"] == a * a


def test_arrow_batched_udf_several_udfs_in_one_operator(
    spark, tmp_path, accelerated, arrow_udf_conf
):
    """Two independent UDFs are planned into a single ArrowEvalPythonExec, and
    the worker returns one top-level column per UDF."""
    data = [(i, float(i)) for i in range(30)]
    df = _write(spark, tmp_path, data, ["id", "value"])

    result = df.select(
        "id",
        "value",
        F.udf(lambda v: v + 1, T.DoubleType())("value").alias("plus_one"),
        F.udf(lambda i: i * 10, T.LongType())("id").alias("times_ten"),
    )

    plan = executed_plan(result)
    _assert_plan_matches_mode(plan, accelerated)
    if accelerated:
        assert plan.count("CometArrowEvalPython") == 1, (
            f"expected the two UDFs to share one operator:\n{plan}"
        )

    rows = result.orderBy("id").collect()
    for row, (i, value) in zip(rows, data):
        assert row["plus_one"] == pytest.approx(value + 1)
        assert row["times_ten"] == i * 10


def test_stacked_operators_of_different_eval_types(
    spark, tmp_path, accelerated, arrow_udf_conf
):
    """`ExtractPythonUDFs` cannot merge UDFs of different eval types into one
    operator, so a `@pandas_udf` and an Arrow-optimized `udf()` stack. The outer
    operator's argument is the column the inner operator's Python worker
    produced, so this covers a native operator consuming another one's output
    with no transition in between.
    """
    pandas = pytest.importorskip("pandas")

    data = [(i,) for i in range(25)]
    df = _write(spark, tmp_path, data, ["a"])

    @F.pandas_udf(T.LongType())
    def plus_one(values: pandas.Series) -> pandas.Series:
        return values + 1

    times_two = F.udf(lambda x: x * 2, T.LongType())
    result = df.withColumn("b", plus_one("a")).withColumn("c", times_two("b"))

    plan = executed_plan(result)
    _assert_plan_matches_mode(plan, accelerated)
    if accelerated:
        assert plan.count("CometArrowEvalPython") == 2, (
            f"expected two stacked native operators:\n{plan}"
        )

    rows = result.orderBy("a").collect()
    for row, (a,) in zip(rows, data):
        assert row["b"] == a + 1
        assert row["c"] == (a + 1) * 2


def test_arrow_batched_udf_empty_input(spark, tmp_path, accelerated, arrow_udf_conf):
    df = _write(spark, tmp_path, [(i,) for i in range(10)], ["a"]).filter("a > 1000")

    result = df.withColumn("b", F.udf(lambda x: x + 1, T.LongType())("a"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)
    assert result.collect() == []


def test_arrow_batched_udf_many_batches(spark, tmp_path, accelerated, arrow_udf_conf):
    """More rows than one Arrow batch, so the input/output batch pairing is
    exercised across several batches."""
    previous = spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "100")
    try:
        data = [(i,) for i in range(1000)]
        df = _write(spark, tmp_path, data, ["a"])
        result = df.withColumn("b", F.udf(lambda x: x + 1, T.LongType())("a"))

        _assert_plan_matches_mode(executed_plan(result), accelerated)

        rows = result.orderBy("a").collect()
        assert len(rows) == 1000
        assert [row["b"] for row in rows] == [a + 1 for (a,) in data]
    finally:
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", previous)


# ---------------------------------------------------------------------------
# Fallback cases: shapes the first version deliberately does not accelerate
# ---------------------------------------------------------------------------


def test_non_attribute_argument_falls_back(spark, tmp_path, arrow_udf_conf):
    """`udf(col + 1)` passes an expression, not a column of the child. Spark does
    not project it below the operator, so Comet leaves the operator to Spark."""
    spark.conf.set("spark.comet.exec.pyarrowUDF.enabled", "true")
    df = _write(spark, tmp_path, [(i,) for i in range(20)], ["a"])

    result = df.withColumn("b", F.udf(lambda x: x * 2, T.LongType())(F.col("a") + 1))

    _assert_falls_back(executed_plan(result))
    rows = result.orderBy("a").collect()
    for row, a in zip(rows, range(20)):
        assert row["b"] == (a + 1) * 2


def test_chained_udf_argument_falls_back(spark, tmp_path, arrow_udf_conf):
    """Two UDFs of the same eval type chained as `f(g(x))` fold into a single
    operator whose second UDF takes the first's result. That argument is a
    PythonUDF, not an attribute of the child, so the operator falls back."""
    spark.conf.set("spark.comet.exec.pyarrowUDF.enabled", "true")
    df = _write(spark, tmp_path, [(i,) for i in range(20)], ["a"])

    plus_one = F.udf(lambda x: x + 1, T.LongType())
    times_two = F.udf(lambda x: x * 2, T.LongType())
    result = df.select(times_two(plus_one("a")).alias("b"))

    plan = executed_plan(result)
    assert plan.count("ArrowEvalPython") == 1, (
        f"expected the chain to fold into one operator:\n{plan}"
    )
    _assert_falls_back(plan)

    assert [row["b"] for row in result.collect()] == [(a + 1) * 2 for a in range(20)]


def test_iterator_pandas_udf_falls_back(spark, tmp_path, arrow_udf_conf):
    """SQL_SCALAR_PANDAS_ITER_UDF guarantees only the total row count, not the
    batching, so it is excluded from the native path."""
    pandas = pytest.importorskip("pandas")
    from typing import Iterator

    spark.conf.set("spark.comet.exec.pyarrowUDF.enabled", "true")
    df = _write(spark, tmp_path, [(i,) for i in range(20)], ["a"])

    @F.pandas_udf(T.LongType())
    def plus_one(batches: Iterator[pandas.Series]) -> Iterator[pandas.Series]:
        for batch in batches:
            yield batch + 1

    result = df.withColumn("b", plus_one("a"))

    _assert_falls_back(executed_plan(result))
    rows = result.orderBy("a").collect()
    assert [row["b"] for row in rows] == [a + 1 for a in range(20)]


def test_large_var_types_falls_back(spark, tmp_path, arrow_udf_conf):
    """Comet's string vectors use 4-byte offsets, so it cannot honour the
    large_string input types this conf asks for."""
    spark.conf.set("spark.comet.exec.pyarrowUDF.enabled", "true")
    previous = spark.conf.get("spark.sql.execution.arrow.useLargeVarTypes")
    spark.conf.set("spark.sql.execution.arrow.useLargeVarTypes", "true")
    try:
        df = _write(spark, tmp_path, [(f"s{i}",) for i in range(20)], ["a"])
        result = df.withColumn("b", F.udf(lambda s: s + "!", T.StringType())("a"))

        _assert_falls_back(executed_plan(result))
        assert [row["b"] for row in result.orderBy("a").collect()] == sorted(
            f"s{i}!" for i in range(20)
        )
    finally:
        spark.conf.set("spark.sql.execution.arrow.useLargeVarTypes", previous)


def test_pickled_udf_is_untouched(spark, tmp_path):
    """With the Arrow conf off, a plain `udf()` is a BatchEvalPythonExec, which
    Comet does not accelerate (there is no columnar boundary to preserve)."""
    spark.conf.set("spark.comet.exec.pyarrowUDF.enabled", "true")
    spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")
    df = _write(spark, tmp_path, [(i,) for i in range(20)], ["a"])

    result = df.withColumn("b", F.udf(lambda x: x + 1, T.LongType())("a"))

    plan = executed_plan(result)
    assert "BatchEvalPython" in plan, f"expected BatchEvalPython in plan:\n{plan}"
    assert "CometArrowEvalPython" not in plan
    assert [row["b"] for row in result.orderBy("a").collect()] == [
        a + 1 for a in range(20)
    ]


# ---------------------------------------------------------------------------
# @pandas_udf scalar (SQL_SCALAR_PANDAS_UDF)
# ---------------------------------------------------------------------------


def test_scalar_pandas_udf(spark, tmp_path, accelerated):
    pandas = pytest.importorskip("pandas")

    data = [(i, float(i) * 1.5) for i in range(120)]
    df = _write(spark, tmp_path, data, ["id", "value"])

    @F.pandas_udf(T.DoubleType())
    def halve(values: pandas.Series) -> pandas.Series:
        return values / 2

    result = df.withColumn("half", halve("value"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("id").collect()
    assert len(rows) == len(data)
    for row, (expected_id, value) in zip(rows, data):
        assert row["id"] == expected_id
        assert row["half"] == pytest.approx(value / 2)


def test_scalar_pandas_udf_string_and_nulls(spark, tmp_path, accelerated):
    pandas = pytest.importorskip("pandas")

    data = [(i, None if i % 4 == 0 else f"v{i}") for i in range(40)]
    df = _write(spark, tmp_path, data, ["id", "name"])

    @F.pandas_udf(T.StringType())
    def suffix(values: pandas.Series) -> pandas.Series:
        # pandas renders a null string as NaN, not None.
        return values.apply(lambda v: None if pandas.isna(v) else v + "_x")

    result = df.withColumn("suffixed", suffix("name"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("id").collect()
    for row, (expected_id, name) in zip(rows, data):
        assert row["id"] == expected_id
        assert row["suffixed"] == (None if name is None else name + "_x")


# ---------------------------------------------------------------------------
# @arrow_udf scalar, Spark 4.1+ (SQL_SCALAR_ARROW_UDF)
# ---------------------------------------------------------------------------


@requires_arrow_udf
def test_scalar_arrow_udf(spark, tmp_path, accelerated):
    import pyarrow as pa
    import pyarrow.compute as pc

    data = [(i, i * 3) for i in range(60)]
    df = _write(spark, tmp_path, data, ["id", "value"])

    @F.arrow_udf(T.LongType())
    def increment(values: pa.Array) -> pa.Array:
        return pc.add(values, 1)

    result = df.withColumn("incremented", increment("value"))

    _assert_plan_matches_mode(executed_plan(result), accelerated)

    rows = result.orderBy("id").collect()
    assert len(rows) == len(data)
    for row, (expected_id, value) in zip(rows, data):
        assert row["id"] == expected_id
        assert row["incremented"] == value + 1

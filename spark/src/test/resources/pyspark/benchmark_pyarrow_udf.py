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
End-to-end wall-clock benchmark for Comet's PyArrow UDF acceleration.

Requires PySpark 4.0.1+ (Comet's columnar runner targets Spark 4.0+ only;
3.5 and 3.4 are documented no-ops).

Times `df.mapInArrow(passthrough, schema).count()` and the equivalent
`mapInPandas` query with `spark.comet.exec.pyarrowUDF.enabled` set
to false (vanilla Spark path) and true (Comet's optimized path). Both
modes run the same Python worker, so the measured delta covers what the
optimization actually changes for users:

  * vanilla:   CometScan -> ColumnarToRow + UnsafeProjection -> ArrowPythonRunner
              (per-row InternalRow.getXXX() loop inside ArrowWriter.write)
  * optimized: CometScan -> CometMapInBatchExec -> CometArrowPythonRunner
              (Arrow IPC serialization directly from Comet's source vectors;
              no row materialization; dictionary inputs use temporary decoded vectors)

Results are wall-clock seconds, so they include Python interpreter,
Arrow IPC, and downstream count() costs. That's intentional: the
optimization's user-visible value is what fraction of end-to-end time
it shaves off, not the JVM-side delta in isolation.

Caveat: the workload here is `passthrough_udf` + `count()` on `local[2]`,
so most of the wall time is Spark's Python fork/IPC overhead with very
little real Python work. Real UDFs (PyArrow compute, pandas ops, model
inference) increase the per-row Python cost, which dilutes the JVM-side
savings and shrinks the speedup ratio relative to what you see here.

Usage:
    # Build Comet (release for representative numbers):
    make release PROFILES='-Pspark-4.0 -Pscala-2.13'

    pip install pyspark==4.0.4 pyarrow pandas

    python3 spark/src/test/resources/pyspark/benchmark_pyarrow_udf.py

Override defaults via environment variables:
    COMET_JAR=/path/to/comet.jar          path to the Comet jar
    BENCHMARK_ROWS=2000000                rows per run
    BENCHMARK_WARMUP=2                    warmup iterations per case
    BENCHMARK_ITERS=5                     measured iterations per case
    BENCHMARK_WORKLOAD='dictionary JVM shuffle'
                                          run only the named workload
"""

import contextlib
import os
import statistics
import sys
import tempfile
import time

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(__file__))
from conftest import resolve_comet_jar


def _build_spark() -> SparkSession:
    jar = resolve_comet_jar()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--jars {jar} --driver-class-path {jar} pyspark-shell"
    )
    return (
        SparkSession.builder.master("local[2]")
        .appName("comet-pyarrow-udf-benchmark")
        .config("spark.plugins", "org.apache.spark.CometPlugin")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        .config(
            "spark.shuffle.manager",
            "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
        )
        # Force the JVM shuffle and a low dictionary threshold so the dictionary workload
        # exercises CometDictionaryVector rather than the native shuffle's plain vectors.
        .config("spark.comet.shuffle.mode", "jvm")
        .config("spark.comet.shuffle.jvm.preferDictionary.ratio", "1.01")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "4g")
        .config("spark.driver.memory", "4g")
        # Pin AQE off so the explain output and plan structure are stable
        # across iterations. AQE doesn't change the optimization's behavior;
        # it just makes plan inspection harder.
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def _passthrough_arrow(iterator):
    for batch in iterator:
        yield batch


def _passthrough_pandas(iterator):
    for pdf in iterator:
        yield pdf


def _narrow_primitives(spark: SparkSession, n: int):
    return spark.range(n).selectExpr(
        "id as id_long",
        "cast(id as int) as id_int",
        "cast(id as double) as id_double",
    )


def _mixed_with_strings(spark: SparkSession, n: int):
    return spark.range(n).selectExpr(
        "id as id_long",
        "cast(id as int) as id_int",
        "cast(id as double) as id_double",
        "concat('row_', cast(id as string)) as id_str",
        "cast(id % 2 as boolean) as id_bool",
    )


def _low_cardinality_strings(spark: SparkSession, n: int):
    return spark.range(n).selectExpr(
        "id as id_long",
        """case
             when id % 23 = 0 then cast(null as string)
             when id % 17 = 0 then ''
             else concat('value_', cast(id % 8 as string))
           end as repeated_str""",
    )


def _wide_rows(spark: SparkSession, n: int):
    types = ["int", "long", "double"]
    cols = [f"cast(id + {i} as {types[i % len(types)]}) as col_{i}" for i in range(50)]
    return spark.range(n).selectExpr(*cols)


WORKLOADS = [
    ("narrow primitives", _narrow_primitives, False),
    ("mixed with strings", _mixed_with_strings, False),
    ("wide rows (50 cols)", _wide_rows, False),
    ("dictionary JVM shuffle", _low_cardinality_strings, True),
]


@contextlib.contextmanager
def _temp_parquet(spark: SparkSession, build_df, n: int):
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "src.parquet")
        build_df(spark, n).write.parquet(path)
        yield path


def _time_run(
    spark: SparkSession,
    parquet_path: str,
    accelerate: bool,
    api: str,
    shuffle_input: bool,
) -> float:
    spark.conf.set(
        "spark.comet.exec.pyarrowUDF.enabled",
        "true" if accelerate else "false",
    )
    df = spark.read.parquet(parquet_path)
    if shuffle_input:
        df = df.repartition(2, "id_long")
    schema = df.schema
    if api == "mapInArrow":
        df = df.mapInArrow(_passthrough_arrow, schema)
    else:
        df = df.mapInPandas(_passthrough_pandas, schema)
    plan = df._jdf.queryExecution().executedPlan().toString()
    if ("CometMapInBatch" in plan) != accelerate:
        expected = "CometMapInBatch" if accelerate else "vanilla Python execution"
        raise RuntimeError(f"Expected {expected} for {api}, but found:\n{plan}")
    if shuffle_input and "CometColumnarExchange" not in plan:
        raise RuntimeError(f"Expected Comet JVM shuffle for {api}, but found:\n{plan}")
    t0 = time.perf_counter()
    df.count()
    return time.perf_counter() - t0


def main() -> None:
    rows = int(os.environ.get("BENCHMARK_ROWS", 1024 * 1024))
    warmup = int(os.environ.get("BENCHMARK_WARMUP", 2))
    iters = int(os.environ.get("BENCHMARK_ITERS", 5))
    workload_name = os.environ.get("BENCHMARK_WORKLOAD")
    workloads = WORKLOADS
    if workload_name:
        workloads = [workload for workload in WORKLOADS if workload[0] == workload_name]
        if not workloads:
            names = ", ".join(name for name, _, _ in WORKLOADS)
            raise ValueError(
                f"Unknown BENCHMARK_WORKLOAD {workload_name!r}; choose from: {names}"
            )

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\nrows per run: {rows:,}")
    print(f"warmup iters: {warmup}, measured iters: {iters}")
    print(f"jar: {resolve_comet_jar()}\n")

    header = "  {:<14} {:<10} {:>10} {:>10} {:>10} {:>13} {:>9}".format(
        "api", "mode", "min (s)", "median (s)", "max (s)", "rows/s", "speedup"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))

    for name, build_df, shuffle_input in workloads:
        print(f"\n=== {name} ===")
        with _temp_parquet(spark, build_df, rows) as parquet_path:
            for api in ("mapInArrow", "mapInPandas"):
                samples_by_mode = {}
                for mode, accelerate in (("vanilla", False), ("optimized", True)):
                    for _ in range(warmup):
                        _time_run(spark, parquet_path, accelerate, api, shuffle_input)
                    samples = [
                        _time_run(spark, parquet_path, accelerate, api, shuffle_input)
                        for _ in range(iters)
                    ]
                    samples_by_mode[mode] = samples
                    median = statistics.median(samples)
                    speedup = ""
                    if mode == "optimized":
                        speedup = "{:.2f}x".format(
                            statistics.median(samples_by_mode["vanilla"]) / median
                        )
                    print(
                        "  {:<14} {:<10} {:>10} {:>10} {:>10} {:>13} {:>9}".format(
                            api,
                            mode,
                            "{:.3f}".format(min(samples)),
                            "{:.3f}".format(median),
                            "{:.3f}".format(max(samples)),
                            "{:,.0f}".format(rows / median),
                            speedup,
                        )
                    )

    spark.stop()


if __name__ == "__main__":
    main()

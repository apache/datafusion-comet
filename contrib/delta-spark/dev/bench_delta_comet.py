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
Benchmark: page-level skipping on a DELTA table under three configurations.

  1. stock     — plain Spark 3.5.6 + delta-spark 3.3.2
  2. comet     — Comet enabled WITHOUT the Delta contrib (scan falls back to Spark)
  3. contrib   — Comet + comet-contrib-delta (native Delta scan)

Writes a 20M-row table sorted by `ts` (4 files, zstd, small pages) as Delta,
optionally deletes a slice via DVs, then runs a 5%-wide range predicate and
reports the fraction of the table materialized by the scan plus wall time.

Usage: python bench_delta_comet.py <mode> <workdir> [--dv] [--subquery]
  mode: stock | comet | contrib  (jars/extensions injected by the wrapper script)
  --subquery: bound the range predicate with scalar subqueries over a one-row
    thresholds Delta table instead of literals. Same rows selected; exercises
    the execution-time resolve-and-push path (which stock Spark 3.5 lacks:
    FileSourceStrategy strips subquery predicates from scan dataFilters).
"""

import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROWS = 20_000_000
FILES = 4
PRED_LO, PRED_HI = 0.475, 0.525  # 5% slice in the middle
# DV delete ranges: one nested inside the predicate slice, one far outside it.
DV_DELETE_LO, DV_DELETE_HI = 0.48, 0.49


def build_session(mode: str) -> SparkSession:
    extensions = "io.delta.sql.DeltaSparkSessionExtension"
    if mode in ("comet", "contrib"):
        extensions += ",org.apache.comet.CometSparkSessionExtensions"
    b = (
        SparkSession.builder.appName(f"delta-comet-bench-{mode}")
        .config("spark.sql.extensions", extensions)
        .config("spark.sql.adaptive.enabled", "false")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.parquet.page.size", str(64 * 1024))
        .config("spark.hadoop.parquet.block.size", str(32 * 1024 * 1024))
    )
    if mode in ("comet", "contrib"):
        b = (
            b.config("spark.comet.enabled", "true")
            .config("spark.comet.exec.enabled", "true")
            .config("spark.comet.exec.shuffle.enabled", "true")
            .config(
                "spark.shuffle.manager",
                "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
            )
            .config("spark.memory.offHeap.enabled", "true")
            .config("spark.memory.offHeap.size", "4g")
            .config("spark.comet.explainFallback.enabled", "true")
        )
    if mode == "contrib":
        b = b.config("spark.comet.scan.delta.enabled", "true")
    return b.getOrCreate()


def write_table(spark: SparkSession, path: str, with_dv: bool) -> None:
    df = (
        spark.range(ROWS)
        .withColumn("ts", F.col("id"))
        .withColumn("payload", F.sha1(F.col("id").cast("string")))
        .repartitionByRange(FILES, "ts")
        .sortWithinPartitions("ts")
    )
    (
        df.write.format("delta")
        .option("compression", "zstd")
        .mode("overwrite")
        .save(path)
    )
    if with_dv:
        spark.sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
            "('delta.enableDeletionVectors' = 'true')"
        )
        lo = int(ROWS * DV_DELETE_LO)
        hi = int(ROWS * DV_DELETE_HI)
        spark.sql(f"DELETE FROM delta.`{path}` WHERE ts >= {lo} AND ts < {hi}")


def scan_metrics(plan):
    """Walk the executed plan and pull metrics from the leaf scan node(s).

    Safe to call right after collect(): the Dataset caches its QueryExecution,
    per-task SQLMetric accumulator updates are merged on the driver before the
    job completes, and AQE is disabled so the executed plan is final.
    """
    from py4j.protocol import Py4JError, Py4JJavaError

    out = {}

    def walk(node):
        try:
            name = node.nodeName()
            if "Scan" in name:
                metrics = node.metrics()
                it = metrics.keysIterator()
                while it.hasNext():
                    k = it.next()
                    out.setdefault((name, k), metrics.get(k).get().value())
            for i in range(node.children().length()):
                walk(node.children().apply(i))
            # innerChildren covers plan-in-plan nodes; entries may not be
            # SparkPlans, so failures here are ignored rather than fatal.
            inner = node.innerChildren()
            for i in range(inner.length()):
                walk(inner.apply(i))
        except (Py4JError, Py4JJavaError):
            pass

    walk(plan)
    return out


def has_native_scan_with_column(plan, column: str) -> bool:
    """True if the executed plan (including subquery inner plans) contains a
    CometDeltaNativeScan whose output includes `column`. Programmatic version of
    the test suite's `output.exists(_.name == col)` check — identifies the MAIN
    table's scan by its distinctive column, since subquery mode adds trivial
    thresholds-table scans that would fool any name-only or count-based check.
    """
    from py4j.protocol import Py4JError, Py4JJavaError

    def walk(node) -> bool:
        try:
            if node.nodeName().startswith("CometDeltaNativeScan"):
                attrs = node.output()
                for i in range(attrs.length()):
                    if attrs.apply(i).name() == column:
                        return True
            for i in range(node.children().length()):
                if walk(node.children().apply(i)):
                    return True
            inner = node.innerChildren()
            for i in range(inner.length()):
                if walk(inner.apply(i)):
                    return True
        except (Py4JError, Py4JJavaError):
            pass
        return False

    return walk(plan)


def pred_bounds() -> tuple[int, int]:
    """Single source of truth for the range bounds, so the literal and subquery
    modes are guaranteed to select the same rows."""
    return int(ROWS * PRED_LO), int(ROWS * PRED_HI)


def write_thresholds(spark: SparkSession, thr_path: str) -> None:
    lo, hi = pred_bounds()
    spark.sql(
        f"SELECT CAST({lo} AS BIGINT) AS lo, CAST({hi} AS BIGINT) AS hi"
    ).write.format("delta").mode("overwrite").save(thr_path)


def run_query(spark: SparkSession, path: str, thr_path: str | None = None):
    if thr_path is not None:
        df = spark.sql(
            f"SELECT count(*) AS n, sum(length(payload)) AS s FROM delta.`{path}` "
            f"WHERE ts >= (SELECT lo FROM delta.`{thr_path}`) "
            f"AND ts < (SELECT hi FROM delta.`{thr_path}`)"
        )
    else:
        lo, hi = pred_bounds()
        df = (
            spark.read.format("delta")
            .load(path)
            .where((F.col("ts") >= lo) & (F.col("ts") < hi))
            .agg(F.count("*").alias("n"), F.sum(F.length("payload")).alias("s"))
        )
    t0 = time.perf_counter()
    row = df.collect()[0]
    elapsed = time.perf_counter() - t0
    plan = df._jdf.queryExecution().executedPlan()
    mets = scan_metrics(plan)
    main_scan_native = has_native_scan_with_column(plan, "payload")
    return row, elapsed, mets, plan.toString(), main_scan_native


def main():
    if len(sys.argv) < 3 or sys.argv[1] not in ("stock", "comet", "contrib"):
        print(__doc__)
        sys.exit(2)
    mode, workdir = sys.argv[1], sys.argv[2]
    with_dv = "--dv" in sys.argv
    with_subquery = "--subquery" in sys.argv
    path = f"{workdir}/delta_bench{'_dv' if with_dv else ''}"
    thr_path = f"{workdir}/delta_bench_thr" if with_subquery else None
    spark = build_session(mode)
    spark.sparkContext.setLogLevel("WARN")

    if not os.path.exists(path + "/_delta_log"):
        print(f"[bench] writing table to {path}")
        write_table(spark, path, with_dv)
    if thr_path is not None and not os.path.exists(thr_path + "/_delta_log"):
        write_thresholds(spark, thr_path)

    try:
        # warm-up then measured run
        run_query(spark, path, thr_path)
        row, elapsed, mets, plan_str, main_scan_native = run_query(spark, path, thr_path)
    except BaseException:
        spark.stop()
        raise

    print(f"\n=== mode={mode} dv={with_dv} subquery={with_subquery} ===")
    print(f"result: n={row['n']} sum={row['s']}")
    print(f"wall_time_s: {elapsed:.3f}")
    interesting = (
        "output_rows",
        "numOutputRows",
        "bytes_scanned",
        "page_index_rows_pruned",
        "page_index_rows_matched",
        "row_groups_pruned_statistics",
        "row_groups_matched_statistics",
        "numFiles",
        "filesSize",
    )
    for (node, k), v in sorted(mets.items()):
        if any(k == i for i in interesting):
            print(f"metric: {node} :: {k} = {v}")
    # rows materialized by the scan as fraction of table
    scanned = [v for (n, k), v in mets.items() if k in ("output_rows", "numOutputRows")]
    if scanned:
        frac = max(scanned) / ROWS
        print(f"scan_fraction: {frac:.4f}")
    seen = {k for (_, k) in mets}
    for key in ("output_rows", "numOutputRows"):
        if key in seen:
            break
    else:
        print("WARNING: no scan row metrics found; scan_fraction unavailable")
    if mode == "contrib" and not main_scan_native:
        print("WARNING: contrib mode but the main table's scan is not CometDeltaNativeScan!")
    spark.stop()


if __name__ == "__main__":
    main()

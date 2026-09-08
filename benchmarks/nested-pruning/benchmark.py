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

"""Generate deterministic nested Parquet data, then time checked Spark queries.

Dependencies: numpy, pyarrow, pyspark matching the Comet build.
Run --generate once; reuse its files for every native library revision.
"""
import argparse
import json
import time
from pathlib import Path


def generate(directory, rows):
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory.mkdir(parents=True, exist_ok=True)
    expected = {}
    for layout in ("sorted", "shuffled"):
        rng = np.random.default_rng(5739)
        keys = np.arange(rows, dtype=np.int64)
        if layout == "shuffled":
            rng.shuffle(keys)
        payload = [rng.integers(0, 1000000, rows, dtype=np.int64) for _ in range(8)]
        inner = pa.StructArray.from_arrays([pa.array(keys)], names=["k"])
        nested = pa.StructArray.from_arrays(
            [inner] + [pa.array(p) for p in payload],
            names=["inner"] + [f"p{i}" for i in range(8)],
        )
        table = pa.table({"k": keys, "s": nested})
        pq.write_table(table, directory / f"{layout}.parquet", row_group_size=65536,
                       compression="snappy", use_dictionary=False, write_statistics=True)
        mask = keys >= rows - 65536
        expected[layout] = {
            "all": [int(p.sum()) for p in payload],
            "selective": [int(p[mask].sum()) for p in payload],
        }
    (directory / "expected.json").write_text(json.dumps({"rows": rows, "sums": expected}))


def metrics(node):
    result = []
    it = node.metrics().iterator()
    values = {}
    while it.hasNext():
        entry = it.next()
        values[entry._1()] = entry._2().value()
    result.append({"node": node.nodeName(), "metrics": values})
    children = node.children().iterator()
    while children.hasNext():
        result.extend(metrics(children.next()))
    return result


def run(args):
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[1]").appName("Comet nested pruning")
             .config("spark.jars", str(args.jar.resolve()))
             .config("spark.driver.extraJavaOptions", f"-Djava.library.path={args.library_dir.resolve()}")
             .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
             .config("spark.shuffle.manager", "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.files.maxPartitionBytes", str(1024**3))
             .config("spark.comet.enabled", "true")
             .config("spark.comet.exec.enabled", "true")
             .config("spark.comet.exec.respectDataFusionConfigs", "true")
             .config("spark.comet.datafusion.execution.parquet.pruning", str(not args.pruning_off).lower())
             .config("spark.comet.datafusion.execution.parquet.enable_page_index", "false")
             .config("spark.comet.datafusion.execution.parquet.bloom_filter_on_read", "false")
             .config("spark.comet.parquet.rowFilterPushdown.enabled", str(args.row_filter).lower())
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    expected = json.loads((args.data / "expected.json").read_text())
    for layout in ("sorted", "shuffled"):
        spark.read.parquet(str((args.data / f"{layout}.parquet").resolve())).createOrReplaceTempView(layout)
    sums = ", ".join(f"sum(s.p{i})" for i in range(8))
    threshold = expected["rows"] - 65536
    cases = [
        ("nested_selective", "sorted", f"s.inner.k >= {threshold}", "selective"),
        ("top_level_selective", "sorted", f"k >= {threshold}", "selective"),
        ("projection_only", "sorted", "true", "all"),
        ("nested_unprunable", "shuffled", f"s.inner.k >= {threshold}", "selective"),
    ]
    output = {"label": args.label, "spark": spark.version, "pruning_off": args.pruning_off,
              "row_filter": args.row_filter, "warmup": args.warmup, "cases": {}}
    for name, layout, predicate, expected_key in cases:
        query = f"SELECT {sums} FROM {layout} WHERE {predicate}"
        samples = []
        for iteration in range(args.runs + args.warmup):
            start = time.perf_counter()
            df = spark.sql(query)
            answer = list(df.collect()[0])
            elapsed = time.perf_counter() - start
            assert answer == expected["sums"][layout][expected_key], (name, answer)
            plan = df._jdf.queryExecution().executedPlan()
            plan_text = plan.toString()
            assert "CometNativeScan" in plan_text, plan_text
            if iteration >= args.warmup:
                samples.append(elapsed)
        scan_metrics = metrics(plan)
        if args.expect_nested_pruned is not None and name == "nested_selective":
            pruned = sum(n["metrics"].get("row_groups_pruned_statistics", 0)
                         for n in scan_metrics if "CometNativeScan" in n["node"])
            assert pruned == args.expect_nested_pruned, scan_metrics
        output["cases"][name] = {"seconds": samples, "query": query, "answer": answer,
                                  "plan": plan_text, "metrics": scan_metrics}
        print(json.dumps({"label": args.label, "case": name, "seconds": samples}), flush=True)
    args.output.write_text(json.dumps(output, indent=2))
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--rows", type=int, default=4194304)
    parser.add_argument("--jar", type=Path)
    parser.add_argument("--library-dir", type=Path)
    parser.add_argument("--label", default="candidate")
    parser.add_argument("--output", type=Path, default=Path("results.json"))
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--pruning-off", action="store_true")
    parser.add_argument("--row-filter", action="store_true")
    parser.add_argument("--expect-nested-pruned", type=int)
    args = parser.parse_args()
    if args.generate:
        generate(args.data, args.rows)
    else:
        run(args)

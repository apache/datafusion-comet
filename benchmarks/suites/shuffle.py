# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Shuffle benchmark suite.

Tests different partitioning strategies (hash, round-robin) across Spark,
Comet JVM, and Comet Native shuffle implementations.
"""

import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession


BENCHMARKS = {
    "shuffle-hash": "Shuffle all columns using hash partitioning on group_key",
    "shuffle-roundrobin": "Shuffle all columns using round-robin partitioning",
}


def _repartition(
    df: DataFrame, benchmark: str, num_partitions: int
) -> DataFrame:
    """Apply the partitioning strategy for the given benchmark."""
    if benchmark == "shuffle-hash":
        return df.repartition(num_partitions, "group_key")
    elif benchmark == "shuffle-roundrobin":
        return df.repartition(num_partitions)
    else:
        raise ValueError(
            f"Unknown shuffle benchmark: {benchmark}. "
            f"Available: {', '.join(BENCHMARKS)}"
        )


def run_shuffle(
    spark: SparkSession,
    benchmark: str,
    data_path: str,
    mode: str,
    num_partitions: int = 200,
    iterations: int = 1,
) -> Dict[str, List[Dict[str, Any]]]:
    """Run a shuffle benchmark and return per-iteration results.

    Returns ``{benchmark_name: [{duration_ms, row_count, num_partitions}, ...]}``
    so the structure parallels TPC output (query -> list of timings).
    """
    if benchmark not in BENCHMARKS:
        raise ValueError(
            f"Unknown shuffle benchmark: {benchmark}. "
            f"Available: {', '.join(BENCHMARKS)}"
        )

    results: List[Dict[str, Any]] = []

    # Read input data once
    df = spark.read.parquet(data_path)
    row_count = df.count()

    for iteration in range(iterations):
        print(f"\n{'=' * 60}")
        print(f"Shuffle benchmark: {benchmark} | Mode: {mode.upper()}")
        print(f"Iteration {iteration + 1} of {iterations}")
        print(f"{'=' * 60}")
        print(f"Data path: {data_path}")
        print(f"Rows: {row_count:,} | Partitions: {num_partitions}")

        # Print relevant Spark configuration
        conf = spark.sparkContext.getConf()
        print(f"Shuffle manager: {conf.get('spark.shuffle.manager', 'default')}")
        print(f"Comet enabled: {conf.get('spark.comet.enabled', 'false')}")
        print(
            f"Comet shuffle enabled: "
            f"{conf.get('spark.comet.exec.shuffle.enabled', 'false')}"
        )
        print(
            f"Comet shuffle mode: "
            f"{conf.get('spark.comet.exec.shuffle.mode', 'not set')}"
        )

        spark.catalog.clearCache()
        spark.sparkContext.setJobDescription(f"{benchmark} iter{iteration + 1}")

        start_time = time.time()

        repartitioned = _repartition(df, benchmark, num_partitions)
        output_path = f"/tmp/shuffle-benchmark-output-{mode}-{benchmark}"
        repartitioned.write.mode("overwrite").parquet(output_path)
        print(f"Wrote repartitioned data to: {output_path}")

        duration_ms = int((time.time() - start_time) * 1000)
        print(f"Duration: {duration_ms:,} ms")

        results.append({
            "duration_ms": duration_ms,
            "row_count": row_count,
            "num_partitions": num_partitions,
        })

    return {benchmark: results}


def build_results(
    spark: SparkSession,
    benchmark: str,
    data_path: str,
    mode: str,
    name: str,
    timings: Dict[str, List[Dict[str, Any]]],
) -> Dict:
    """Assemble the result dict for shuffle benchmarks."""
    conf_dict = {k: v for k, v in spark.sparkContext.getConf().getAll()}

    return {
        "engine": "datafusion-comet",
        "benchmark": benchmark,
        "mode": mode,
        "data_path": data_path,
        "spark_conf": conf_dict,
        **timings,
    }


def write_results(
    results: Dict,
    output_dir: str,
    name: str,
    benchmark: str,
) -> str:
    """Write JSON results file.  Returns the path written."""
    result_str = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"{output_dir}/{name}-{benchmark}-{current_time_millis}.json"
    print(f"\nWriting results to {results_path}")
    with open(results_path, "w") as f:
        f.write(result_str)
    return results_path

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
Microbenchmark suite.

Ports expression-level benchmarks (e.g. CometStringExpressionBenchmark) to the
unified runner.  Each benchmark generates a small dataset, runs SQL expressions
in a tight loop, and records per-iteration wall-clock times.
"""

import json
import os
import shutil
import tempfile
import time
from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# String expression benchmark
# ---------------------------------------------------------------------------

STRING_EXPRESSIONS: List[tuple] = [
    ("ascii", "select ascii(c1) from parquetV1Table"),
    ("bit_length", "select bit_length(c1) from parquetV1Table"),
    ("btrim", "select btrim(c1) from parquetV1Table"),
    ("chr", "select chr(c1) from parquetV1Table"),
    ("concat", "select concat(c1, c1) from parquetV1Table"),
    ("concat_ws", "select concat_ws(' ', c1, c1) from parquetV1Table"),
    ("contains", "select contains(c1, '123') from parquetV1Table"),
    ("endswith", "select endswith(c1, '9') from parquetV1Table"),
    ("initcap", "select initCap(c1) from parquetV1Table"),
    ("instr", "select instr(c1, '123') from parquetV1Table"),
    ("length", "select length(c1) from parquetV1Table"),
    ("like", "select c1 like '%123%' from parquetV1Table"),
    ("lower", "select lower(c1) from parquetV1Table"),
    ("lpad", "select lpad(c1, 150, 'x') from parquetV1Table"),
    ("ltrim", "select ltrim(c1) from parquetV1Table"),
    ("octet_length", "select octet_length(c1) from parquetV1Table"),
    ("regexp_replace", "select regexp_replace(c1, '[0-9]', 'X') from parquetV1Table"),
    ("repeat", "select repeat(c1, 3) from parquetV1Table"),
    ("replace", "select replace(c1, '123', 'ab') from parquetV1Table"),
    ("reverse", "select reverse(c1) from parquetV1Table"),
    ("rlike", "select c1 rlike '[0-9]+' from parquetV1Table"),
    ("rpad", "select rpad(c1, 150, 'x') from parquetV1Table"),
    ("rtrim", "select rtrim(c1) from parquetV1Table"),
    ("space", "select space(2) from parquetV1Table"),
    ("startswith", "select startswith(c1, '1') from parquetV1Table"),
    ("substring", "select substring(c1, 1, 100) from parquetV1Table"),
    ("translate", "select translate(c1, '123456', 'aBcDeF') from parquetV1Table"),
    ("trim", "select trim(c1) from parquetV1Table"),
    ("upper", "select upper(c1) from parquetV1Table"),
]

BENCHMARKS = {
    "string-expressions": "String expression microbenchmarks (29 expressions)",
}


def prepare_string_table(
    spark: SparkSession, num_rows: int, temp_dir: str
) -> None:
    """Generate a string column table and register it as ``parquetV1Table``."""
    path = os.path.join(temp_dir, "string_data")
    spark.range(num_rows).selectExpr(
        "REPEAT(CAST(id AS STRING), 10) AS c1"
    ).write.mode("overwrite").option("compression", "snappy").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView("parquetV1Table")
    print(f"Generated {num_rows} rows in {path}")


def run_micro(
    spark: SparkSession,
    benchmark: str,
    num_rows: int = 1024,
    iterations: int = 3,
    expression: Optional[str] = None,
) -> Dict[str, List[float]]:
    """Run a microbenchmark and return ``{expr_name: [elapsed_secs, ...]}``."""
    if benchmark != "string-expressions":
        raise ValueError(
            f"Unknown micro benchmark: {benchmark}. "
            f"Available: {', '.join(BENCHMARKS)}"
        )

    temp_dir = tempfile.mkdtemp(prefix="comet-micro-")
    try:
        prepare_string_table(spark, num_rows, temp_dir)

        expressions = STRING_EXPRESSIONS
        if expression is not None:
            expressions = [(n, sql) for n, sql in expressions if n == expression]
            if not expressions:
                valid = [n for n, _ in STRING_EXPRESSIONS]
                raise ValueError(
                    f"Unknown expression: {expression}. Valid: {', '.join(valid)}"
                )

        timings: Dict[str, List[float]] = {}

        for expr_name, sql in expressions:
            print(f"\n{'=' * 60}")
            print(f"Expression: {expr_name}")
            print(f"{'=' * 60}")

            for iteration in range(iterations):
                spark.sparkContext.setJobDescription(
                    f"{benchmark} {expr_name} iter{iteration + 1}"
                )
                start = time.time()
                spark.sql(sql).foreach(lambda _: None)
                elapsed = time.time() - start
                print(f"  Iteration {iteration + 1}: {elapsed:.4f}s")
                timings.setdefault(expr_name, []).append(elapsed)

        return timings
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def build_results(
    spark: SparkSession,
    benchmark: str,
    name: str,
    timings: Dict[str, List[float]],
) -> Dict:
    """Assemble the result dict for micro benchmarks."""
    conf_dict = {k: v for k, v in spark.sparkContext.getConf().getAll()}

    results: Dict = {
        "engine": "datafusion-comet",
        "benchmark": benchmark,
        "spark_conf": conf_dict,
    }
    for expr_name, elapsed_list in timings.items():
        results[expr_name] = elapsed_list

    return results


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

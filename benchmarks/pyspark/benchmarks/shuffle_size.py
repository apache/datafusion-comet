#!/usr/bin/env python3
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
Shuffle size benchmark for measuring shuffle write bytes.

Measures the actual shuffle file sizes on disk to compare
shuffle file sizes between Spark and Comet shuffle implementations.
This is useful for investigating shuffle format overhead (see issue #3882).

The benchmark sets spark.local.dir to a dedicated temp directory and
measures the total size of shuffle data files (.data) written there.
"""

import json
import os
import urllib.request
from typing import Dict, Any

from pyspark.sql import DataFrame

from .base import Benchmark


def get_shuffle_write_bytes(spark) -> int:
    """Get total shuffle write bytes from the Spark REST API."""
    sc = spark.sparkContext
    ui_url = sc.uiWebUrl
    url = f"{ui_url}/api/v1/applications/{sc.applicationId}/stages"
    with urllib.request.urlopen(url) as resp:
        stages = json.loads(resp.read())
    return sum(s.get("shuffleWriteBytes", 0) for s in stages)


def get_shuffle_disk_bytes(local_dir: str) -> int:
    """Walk spark.local.dir and sum the sizes of all shuffle .data files."""
    total = 0
    for root, _dirs, files in os.walk(local_dir):
        for f in files:
            if f.endswith(".data"):
                total += os.path.getsize(os.path.join(root, f))
    return total


def format_bytes(b: int) -> str:
    """Format byte count as human-readable string."""
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.2f} GiB"
    elif b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.2f} MiB"
    else:
        return f"{b / 1024:.2f} KiB"


class ShuffleSizeBenchmark(Benchmark):
    """
    Benchmark that measures shuffle write bytes on disk.

    Runs a simple scan -> repartition -> count pipeline and reports
    the actual shuffle data file sizes alongside the Spark REST API
    metric. Useful for comparing shuffle format overhead between
    Spark and Comet.

    NOTE: The Spark session must be configured with spark.local.dir
    pointing to a dedicated empty directory so that we can measure
    shuffle file sizes accurately. The run_shuffle_size_benchmark.sh
    script handles this automatically.
    """

    def __init__(self, spark, data_path: str, mode: str,
                 num_partitions: int = 200):
        super().__init__(spark, data_path, mode)
        self.num_partitions = num_partitions

    @classmethod
    def name(cls) -> str:
        return "shuffle-size"

    @classmethod
    def description(cls) -> str:
        return "Measure shuffle write bytes (scan -> repartition -> count)"

    def run(self) -> Dict[str, Any]:
        df = self.spark.read.parquet(self.data_path)
        row_count = df.count()
        print(f"Input rows: {row_count:,}")

        schema_desc = ", ".join(
            f"{f.name}: {f.dataType.simpleString()}" for f in df.schema.fields
        )
        print(f"Schema: {schema_desc}")

        # Read spark.local.dir so we can measure shuffle files on disk
        local_dir = self.spark.sparkContext.getConf().get(
            "spark.local.dir", "/tmp"
        )

        output_path = (
            f"/tmp/shuffle-size-benchmark-output-{self.mode}"
        )

        def benchmark_operation():
            df.repartition(self.num_partitions).write.mode(
                "overwrite"
            ).parquet(output_path)

        duration_ms = self._time_operation(benchmark_operation)

        # Measure actual shuffle file sizes on disk.
        # Shuffle .data files persist until SparkContext shutdown,
        # so they are still available after the job completes.
        disk_bytes = get_shuffle_disk_bytes(local_dir)

        # Also grab the REST API metric for comparison
        api_bytes = 0
        try:
            api_bytes = get_shuffle_write_bytes(self.spark)
        except Exception as e:
            print(f"Warning: could not read shuffle metrics from REST API: {e}")

        disk_bpr = disk_bytes / row_count if row_count > 0 else 0
        api_bpr = api_bytes / row_count if row_count > 0 else 0

        print(f"Shuffle disk:       {format_bytes(disk_bytes)} "
              f"({disk_bpr:.1f} B/record)")
        print(f"Shuffle API metric: {format_bytes(api_bytes)} "
              f"({api_bpr:.1f} B/record)")

        return {
            "duration_ms": duration_ms,
            "row_count": row_count,
            "num_partitions": self.num_partitions,
            "shuffle_disk_bytes": disk_bytes,
            "shuffle_disk_bytes_per_record": round(disk_bpr, 1),
            "shuffle_api_bytes": api_bytes,
            "shuffle_api_bytes_per_record": round(api_bpr, 1),
        }

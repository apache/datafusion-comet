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

Measures the actual shuffle write bytes reported by Spark to compare
shuffle file sizes between Spark and Comet shuffle implementations.
This is useful for investigating shuffle format overhead (see issue #3882).
"""

import json
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
    Benchmark that measures shuffle write bytes via the Spark REST API.

    Runs a simple scan -> repartition -> write pipeline and reports
    the shuffle write size alongside wall-clock time. Useful for
    comparing shuffle format overhead between Spark and Comet.
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
        return "Measure shuffle write bytes (scan -> repartition -> write)"

    def run(self) -> Dict[str, Any]:
        df = self.spark.read.parquet(self.data_path)
        row_count = df.count()
        print(f"Input rows: {row_count:,}")

        schema_desc = ", ".join(
            f"{f.name}: {f.dataType.simpleString()}" for f in df.schema.fields
        )
        print(f"Schema: {schema_desc}")

        output_path = (
            f"/tmp/shuffle-size-benchmark-output-{self.mode}"
        )

        def benchmark_operation():
            df.repartition(self.num_partitions).write.mode(
                "overwrite"
            ).parquet(output_path)

        duration_ms = self._time_operation(benchmark_operation)

        shuffle_write_bytes = 0
        try:
            shuffle_write_bytes = get_shuffle_write_bytes(self.spark)
        except Exception as e:
            print(f"Warning: could not read shuffle metrics: {e}")

        bytes_per_record = (
            shuffle_write_bytes / row_count if row_count > 0 else 0
        )

        print(f"Shuffle write: {format_bytes(shuffle_write_bytes)}")
        print(f"Bytes/record:  {bytes_per_record:.1f}")

        return {
            "duration_ms": duration_ms,
            "row_count": row_count,
            "num_partitions": self.num_partitions,
            "shuffle_write_bytes": shuffle_write_bytes,
            "bytes_per_record": round(bytes_per_record, 1),
        }

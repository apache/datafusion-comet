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
Shuffle benchmarks for comparing shuffle file sizes and performance.

These benchmarks test different partitioning strategies (hash, round-robin)
across Spark, Comet JVM, and Comet Native shuffle implementations.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame

from .base import Benchmark


class ShuffleBenchmark(Benchmark):
    """Base class for shuffle benchmarks with common repartitioning logic."""

    def __init__(self, spark, data_path: str, mode: str, num_partitions: int = 200):
        """
        Initialize shuffle benchmark.

        Args:
            spark: SparkSession instance
            data_path: Path to input parquet data
            mode: Execution mode (spark, jvm, native)
            num_partitions: Number of partitions to shuffle to
        """
        super().__init__(spark, data_path, mode)
        self.num_partitions = num_partitions

    def _read_and_count(self) -> tuple[DataFrame, int]:
        """Read input data and count rows."""
        df = self.spark.read.parquet(self.data_path)
        row_count = df.count()
        return df, row_count

    def _repartition(self, df: DataFrame) -> DataFrame:
        """
        Repartition dataframe using the strategy defined by subclass.

        Args:
            df: Input dataframe

        Returns:
            Repartitioned dataframe
        """
        raise NotImplementedError("Subclasses must implement _repartition")

    def _write_output(self, df: DataFrame, output_path: str):
        """Write repartitioned data to parquet."""
        df.write.mode("overwrite").parquet(output_path)

    def run(self) -> Dict[str, Any]:
        """
        Run the shuffle benchmark.

        Returns:
            Dictionary with duration_ms and row_count
        """
        # Read input data
        df, row_count = self._read_and_count()
        print(f"Number of rows: {row_count:,}")

        # Define the benchmark operation
        def benchmark_operation():
            # Repartition using the specific strategy
            repartitioned = self._repartition(df)

            # Write to parquet to force materialization
            output_path = f"/tmp/shuffle-benchmark-output-{self.mode}-{self.name()}"
            self._write_output(repartitioned, output_path)
            print(f"Wrote repartitioned data to: {output_path}")

        # Time the operation
        duration_ms = self._time_operation(benchmark_operation)

        return {
            'duration_ms': duration_ms,
            'row_count': row_count,
            'num_partitions': self.num_partitions,
        }


class ShuffleHashBenchmark(ShuffleBenchmark):
    """Shuffle benchmark using hash partitioning on a key column."""

    @classmethod
    def name(cls) -> str:
        return "shuffle-hash"

    @classmethod
    def description(cls) -> str:
        return "Shuffle all columns using hash partitioning on group_key"

    def _repartition(self, df: DataFrame) -> DataFrame:
        """Repartition using hash partitioning on group_key."""
        return df.repartition(self.num_partitions, "group_key")


class ShuffleRoundRobinBenchmark(ShuffleBenchmark):
    """Shuffle benchmark using round-robin partitioning."""

    @classmethod
    def name(cls) -> str:
        return "shuffle-roundrobin"

    @classmethod
    def description(cls) -> str:
        return "Shuffle all columns using round-robin partitioning"

    def _repartition(self, df: DataFrame) -> DataFrame:
        """Repartition using round-robin (no partition columns specified)."""
        return df.repartition(self.num_partitions)

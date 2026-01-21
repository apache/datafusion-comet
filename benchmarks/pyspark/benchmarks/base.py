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
Base benchmark class providing common functionality for all benchmarks.
"""

import time
from abc import ABC, abstractmethod
from typing import Dict, Any

from pyspark.sql import SparkSession


class Benchmark(ABC):
    """Base class for all PySpark benchmarks."""

    def __init__(self, spark: SparkSession, data_path: str, mode: str):
        """
        Initialize benchmark.

        Args:
            spark: SparkSession instance
            data_path: Path to input data
            mode: Execution mode (spark, jvm, native)
        """
        self.spark = spark
        self.data_path = data_path
        self.mode = mode

    @classmethod
    @abstractmethod
    def name(cls) -> str:
        """Return the benchmark name (used for CLI)."""
        pass

    @classmethod
    @abstractmethod
    def description(cls) -> str:
        """Return a short description of the benchmark."""
        pass

    @abstractmethod
    def run(self) -> Dict[str, Any]:
        """
        Run the benchmark and return results.

        Returns:
            Dictionary containing benchmark results (must include 'duration_ms')
        """
        pass

    def execute_timed(self) -> Dict[str, Any]:
        """
        Execute the benchmark with timing and standard output.

        Returns:
            Dictionary containing benchmark results
        """
        print(f"\n{'=' * 80}")
        print(f"Benchmark: {self.name()}")
        print(f"Mode: {self.mode.upper()}")
        print(f"{'=' * 80}")
        print(f"Data path: {self.data_path}")

        # Print relevant Spark configuration
        self._print_spark_config()

        # Clear cache before running
        self.spark.catalog.clearCache()

        # Run the benchmark
        print(f"\nRunning benchmark...")
        results = self.run()

        # Print results
        print(f"\nDuration: {results['duration_ms']:,} ms")
        if 'row_count' in results:
            print(f"Rows processed: {results['row_count']:,}")

        # Print any additional metrics
        for key, value in results.items():
            if key not in ['duration_ms', 'row_count']:
                print(f"{key}: {value}")

        print(f"{'=' * 80}\n")

        return results

    def _print_spark_config(self):
        """Print relevant Spark configuration."""
        conf = self.spark.sparkContext.getConf()
        print(f"Shuffle manager: {conf.get('spark.shuffle.manager', 'default')}")
        print(f"Comet enabled: {conf.get('spark.comet.enabled', 'false')}")
        print(f"Comet shuffle enabled: {conf.get('spark.comet.exec.shuffle.enabled', 'false')}")
        print(f"Comet shuffle mode: {conf.get('spark.comet.shuffle.mode', 'not set')}")
        print(f"Spark UI: {self.spark.sparkContext.uiWebUrl}")

    def _time_operation(self, operation_fn):
        """
        Time an operation and return duration in milliseconds.

        Args:
            operation_fn: Function to time (takes no arguments)

        Returns:
            Duration in milliseconds
        """
        start_time = time.time()
        operation_fn()
        duration_ms = int((time.time() - start_time) * 1000)
        return duration_ms

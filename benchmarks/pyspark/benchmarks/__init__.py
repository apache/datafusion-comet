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
Benchmark registry for PySpark benchmarks.

This module provides a central registry for discovering and running benchmarks.
"""

from typing import Dict, Type, List

from .base import Benchmark
from .shuffle import ShuffleHashBenchmark, ShuffleRoundRobinBenchmark


# Registry of all available benchmarks
_BENCHMARK_REGISTRY: Dict[str, Type[Benchmark]] = {
    ShuffleHashBenchmark.name(): ShuffleHashBenchmark,
    ShuffleRoundRobinBenchmark.name(): ShuffleRoundRobinBenchmark,
}


def get_benchmark(name: str) -> Type[Benchmark]:
    """
    Get a benchmark class by name.

    Args:
        name: Benchmark name

    Returns:
        Benchmark class

    Raises:
        KeyError: If benchmark name is not found
    """
    if name not in _BENCHMARK_REGISTRY:
        available = ", ".join(sorted(_BENCHMARK_REGISTRY.keys()))
        raise KeyError(
            f"Unknown benchmark: {name}. Available benchmarks: {available}"
        )
    return _BENCHMARK_REGISTRY[name]


def list_benchmarks() -> List[tuple[str, str]]:
    """
    List all available benchmarks.

    Returns:
        List of (name, description) tuples
    """
    benchmarks = []
    for name in sorted(_BENCHMARK_REGISTRY.keys()):
        benchmark_cls = _BENCHMARK_REGISTRY[name]
        benchmarks.append((name, benchmark_cls.description()))
    return benchmarks


__all__ = [
    'Benchmark',
    'get_benchmark',
    'list_benchmarks',
    'ShuffleHashBenchmark',
    'ShuffleRoundRobinBenchmark',
]

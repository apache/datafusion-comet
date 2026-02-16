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
Parse profiling output and generate memory utilisation reports.

Supports two data sources:

1. **Spark REST API metrics** — CSV written by ``runner/profiling.py``
   (``SparkMetricsProfiler``).  Columns include ``elapsed_secs``,
   ``executor_id``, ``memoryUsed``, ``maxMemory``, and various peak metrics.

2. **Container cgroup metrics** — CSV written by
   ``infra/docker/collect-metrics.sh``.  Columns:
   ``timestamp_ms, memory_usage_bytes, memory_limit_bytes, rss_bytes,
   cache_bytes, swap_bytes``.

Usage::

    python -m benchmarks.analysis.memory_report \\
        --spark-csv results/comet-tpch-metrics.csv \\
        --container-csv results/container-metrics.csv \\
        --output-dir ./charts
"""

import argparse
import csv
import os
import sys
from typing import Dict, List, Optional

import matplotlib.pyplot as plt


# ---------------------------------------------------------------------------
# Spark REST API metrics
# ---------------------------------------------------------------------------

def parse_spark_csv(path: str) -> Dict[str, List[Dict]]:
    """Parse a SparkMetricsProfiler CSV into per-executor time series.

    Returns ``{executor_id: [{elapsed_secs, memoryUsed, maxMemory, ...}]}``
    """
    executors: Dict[str, List[Dict]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            eid = row.get("executor_id", "unknown")
            parsed = {}
            for k, v in row.items():
                try:
                    parsed[k] = float(v)
                except (ValueError, TypeError):
                    parsed[k] = v
            executors.setdefault(eid, []).append(parsed)
    return executors


def generate_spark_memory_chart(
    spark_csv: str,
    output_dir: str = ".",
) -> List[str]:
    """Generate per-executor memory usage over time.  Returns output paths."""
    executors = parse_spark_csv(spark_csv)
    paths = []

    for eid, samples in executors.items():
        elapsed = [s.get("elapsed_secs", 0) for s in samples]
        used = [s.get("memoryUsed", 0) / (1024 ** 2) for s in samples]  # MB
        max_mem = [s.get("maxMemory", 0) / (1024 ** 2) for s in samples]

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(elapsed, used, label="memoryUsed", linewidth=1.5)
        if any(m > 0 for m in max_mem):
            ax.plot(elapsed, max_mem, label="maxMemory", linestyle="--", alpha=0.6)
        ax.set_xlabel("Elapsed (seconds)")
        ax.set_ylabel("Memory (MB)")
        ax.set_title(f"Executor {eid} — JVM Memory Usage")
        ax.legend()
        ax.grid(True, alpha=0.3)

        fname = f"spark_memory_executor_{eid}.png"
        path = os.path.join(output_dir, fname)
        plt.savefig(path, format="png")
        plt.close(fig)
        paths.append(path)

    # Peak memory bar chart across executors
    if executors:
        fig, ax = plt.subplots(figsize=(max(6, len(executors) * 1.5), 5))
        eids = list(executors.keys())
        peaks = []
        for eid in eids:
            peak = max(
                (s.get("peak_JVMHeapMemory", 0) + s.get("peak_JVMOffHeapMemory", 0))
                for s in executors[eid]
            ) / (1024 ** 2)
            peaks.append(peak)

        bars = ax.bar(eids, peaks, color="coral")
        for bar, val in zip(bars, peaks):
            ax.text(
                bar.get_x() + bar.get_width() / 2.0, val,
                f"{val:.0f}", va="bottom", ha="center", fontsize=9,
            )
        ax.set_xlabel("Executor")
        ax.set_ylabel("Peak JVM Memory (MB)")
        ax.set_title("Peak JVM Memory by Executor")
        ax.grid(True, axis="y", alpha=0.3)

        path = os.path.join(output_dir, "spark_memory_peak.png")
        plt.savefig(path, format="png")
        plt.close(fig)
        paths.append(path)

    for p in paths:
        print(f"Wrote {p}")
    return paths


# ---------------------------------------------------------------------------
# Container cgroup metrics
# ---------------------------------------------------------------------------

def parse_container_csv(path: str) -> List[Dict[str, float]]:
    """Parse a collect-metrics.sh CSV into a list of samples."""
    samples = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = {}
            for k, v in row.items():
                try:
                    parsed[k] = float(v)
                except (ValueError, TypeError):
                    parsed[k] = v
            samples.append(parsed)
    return samples


def generate_container_memory_chart(
    container_csv: str,
    output_dir: str = ".",
) -> List[str]:
    """Generate container memory usage over time.  Returns output paths."""
    samples = parse_container_csv(container_csv)
    if not samples:
        print("No container metrics samples found")
        return []

    t0 = samples[0].get("timestamp_ms", 0)
    elapsed = [(s.get("timestamp_ms", 0) - t0) / 1000.0 for s in samples]
    usage_mb = [s.get("memory_usage_bytes", 0) / (1024 ** 2) for s in samples]
    rss_mb = [s.get("rss_bytes", 0) / (1024 ** 2) for s in samples]
    cache_mb = [s.get("cache_bytes", 0) / (1024 ** 2) for s in samples]
    limit_mb = [s.get("memory_limit_bytes", 0) / (1024 ** 2) for s in samples]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(elapsed, usage_mb, label="total usage", linewidth=1.5)
    ax.plot(elapsed, rss_mb, label="RSS", linewidth=1.2)
    ax.plot(elapsed, cache_mb, label="cache", linewidth=1.0, alpha=0.7)
    if any(m > 0 for m in limit_mb):
        ax.axhline(
            limit_mb[0], color="red", linestyle="--", linewidth=1.0,
            label=f"limit ({limit_mb[0]:.0f} MB)",
        )
    ax.set_xlabel("Elapsed (seconds)")
    ax.set_ylabel("Memory (MB)")
    ax.set_title("Container Memory Usage (cgroup)")
    ax.legend()
    ax.grid(True, alpha=0.3)

    paths = []
    path = os.path.join(output_dir, "container_memory.png")
    plt.savefig(path, format="png")
    plt.close(fig)
    paths.append(path)

    # Summary stats
    peak_usage = max(usage_mb)
    peak_rss = max(rss_mb)
    limit = limit_mb[0] if limit_mb else 0
    print(f"Container memory summary:")
    print(f"  Peak usage:  {peak_usage:.0f} MB")
    print(f"  Peak RSS:    {peak_rss:.0f} MB")
    if limit > 0:
        print(f"  Limit:       {limit:.0f} MB")
        print(f"  Peak % used: {peak_usage / limit * 100:.1f}%")

    for p in paths:
        print(f"Wrote {p}")
    return paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate memory utilisation reports from profiling data",
    )
    parser.add_argument(
        "--spark-csv", help="Path to SparkMetricsProfiler CSV",
    )
    parser.add_argument(
        "--container-csv", help="Path to collect-metrics.sh CSV",
    )
    parser.add_argument(
        "--output-dir", default=".", help="Directory for chart PNGs",
    )
    args = parser.parse_args(argv)

    if not args.spark_csv and not args.container_csv:
        parser.error("At least one of --spark-csv or --container-csv is required")

    os.makedirs(args.output_dir, exist_ok=True)

    if args.spark_csv:
        generate_spark_memory_chart(args.spark_csv, args.output_dir)
    if args.container_csv:
        generate_container_memory_chart(args.container_csv, args.output_dir)


if __name__ == "__main__":
    main()

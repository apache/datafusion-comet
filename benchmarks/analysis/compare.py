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
Compare benchmark results and generate charts.

Reads the JSON output produced by ``suites/tpc.py`` (integer query keys
serialised as strings by ``json.dumps``).

Usage::

    python -m benchmarks.analysis.compare \\
        comet-tpch-*.json spark-tpch-*.json \\
        --labels comet spark --benchmark tpch --title "SF100" \\
        --output-dir ./charts
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np


QUERY_COUNTS = {"tpch": 22, "tpcds": 99}


def _query_range(benchmark: str) -> range:
    n = QUERY_COUNTS.get(benchmark)
    if n is None:
        raise ValueError(f"Unknown benchmark: {benchmark}")
    return range(1, n + 1)


def _median(timings: List[float]) -> float:
    return float(np.median(np.array(timings)))


# ---------------------------------------------------------------------------
# Chart generators
# ---------------------------------------------------------------------------

def generate_summary_chart(
    results: Sequence[Dict[str, Any]],
    labels: Sequence[str],
    benchmark: str,
    title: str,
    output_dir: str = ".",
) -> str:
    """Total wall-clock bar chart.  Returns the output path."""
    num_queries = QUERY_COUNTS[benchmark]
    timings = [0.0] * len(results)
    for query in _query_range(benchmark):
        for i, r in enumerate(results):
            timings[i] += _median(r[str(query)])

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_title(title)
    ax.set_ylabel(
        f"Time in seconds to run all {num_queries} {benchmark} queries "
        f"(lower is better)"
    )
    times = [round(x, 0) for x in timings]
    bars = ax.bar(labels, times, color="skyblue", width=0.8)
    for bar in bars:
        yval = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0, yval, f"{yval}",
            va="bottom", ha="center",
        )
    path = os.path.join(output_dir, f"{benchmark}_allqueries.png")
    plt.savefig(path, format="png")
    plt.close(fig)
    return path


def generate_comparison_chart(
    results: Sequence[Dict[str, Any]],
    labels: Sequence[str],
    benchmark: str,
    title: str,
    output_dir: str = ".",
) -> str:
    """Per-query grouped bar chart.  Returns the output path."""
    queries: List[str] = []
    benches: List[List[float]] = [[] for _ in results]
    for query in _query_range(benchmark):
        queries.append(f"q{query}")
        for i, r in enumerate(results):
            benches[i].append(_median(r[str(query)]))

    bar_width = 0.3
    index = np.arange(len(queries)) * 1.5
    fig_w = 15 if benchmark == "tpch" else 35
    fig, ax = plt.subplots(figsize=(fig_w, 6))

    for i, label in enumerate(labels):
        ax.bar(index + i * bar_width, benches[i], bar_width, label=label)

    ax.set_title(title)
    ax.set_xlabel("Queries")
    ax.set_ylabel("Query Time (seconds)")
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(queries)
    ax.legend()

    path = os.path.join(output_dir, f"{benchmark}_queries_compare.png")
    plt.savefig(path, format="png")
    plt.close(fig)
    return path


def _speedup_data(
    baseline: Dict, comparison: Dict, benchmark: str, absolute: bool,
) -> Tuple[List[str], List[float]]:
    """Compute per-query speedup (relative % or absolute seconds)."""
    rows: List[Tuple[str, float]] = []
    for query in _query_range(benchmark):
        a = _median(baseline[str(query)])
        b = _median(comparison[str(query)])
        if absolute:
            rows.append((f"q{query}", round(a - b, 1)))
        else:
            if a > b:
                speedup = a / b - 1
            else:
                speedup = -(1 / (a / b) - 1)
            rows.append((f"q{query}", round(speedup * 100, 0)))
    rows.sort(key=lambda x: -x[1])
    qs, vals = zip(*rows)
    return list(qs), list(vals)


def generate_speedup_chart(
    baseline: Dict[str, Any],
    comparison: Dict[str, Any],
    label1: str,
    label2: str,
    benchmark: str,
    title: str,
    absolute: bool = False,
    output_dir: str = ".",
) -> str:
    """Relative (%) or absolute (seconds) speedup chart.  Returns path."""
    queries, speedups = _speedup_data(baseline, comparison, benchmark, absolute)

    fig_w = 10 if benchmark == "tpch" else 35
    fig_h = 6 if benchmark == "tpch" else 10
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    bars = ax.bar(queries, speedups, color="skyblue")

    for bar, val in zip(bars, speedups):
        yval = bar.get_height()
        fmt = f"{val:.1f}" if absolute else f"{val:.0f}%"
        va = "bottom" if yval >= 0 else "top"
        y = min(800, yval + 5) if yval >= 0 else yval
        ax.text(
            bar.get_x() + bar.get_width() / 2.0, y, fmt,
            va=va, ha="center", fontsize=8, color="blue", rotation=90,
        )

    kind = "seconds" if absolute else "percentage"
    suffix = "abs" if absolute else "rel"
    ylabel = "Speedup (in seconds)" if absolute else "Speedup Percentage (100% speedup = 2x faster)"
    ax.set_title(f"{label2} speedup over {label1} ({title})")
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Query")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.yaxis.grid(True)

    if not absolute:
        min_val = (min(speedups) // 100) * 100
        max_val = ((max(speedups) // 100) + 1) * 100 + 50
        if benchmark == "tpch":
            ax.set_ylim(min_val, max_val)
        else:
            ax.set_ylim(-250, 300)
    else:
        ax.set_ylim(min(speedups) * 2 - 20, max(speedups) * 1.5)

    path = os.path.join(output_dir, f"{benchmark}_queries_speedup_{suffix}.png")
    plt.savefig(path, format="png")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compare(
    files: Sequence[str],
    labels: Sequence[str],
    benchmark: str,
    title: str,
    output_dir: str = ".",
) -> List[str]:
    """Run all applicable charts.  Returns list of output file paths."""
    os.makedirs(output_dir, exist_ok=True)
    results = []
    for filename in files:
        with open(filename) as f:
            results.append(json.load(f))

    paths = [
        generate_summary_chart(results, labels, benchmark, title, output_dir),
        generate_comparison_chart(results, labels, benchmark, title, output_dir),
    ]

    if len(files) == 2:
        paths.append(
            generate_speedup_chart(
                results[0], results[1], labels[0], labels[1],
                benchmark, title, absolute=True, output_dir=output_dir,
            )
        )
        paths.append(
            generate_speedup_chart(
                results[0], results[1], labels[0], labels[1],
                benchmark, title, absolute=False, output_dir=output_dir,
            )
        )

    for p in paths:
        print(f"Wrote {p}")
    return paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compare benchmark results and generate charts",
    )
    parser.add_argument("filenames", nargs="+", help="JSON result files")
    parser.add_argument("--labels", nargs="+", required=True, help="Labels for each file")
    parser.add_argument("--benchmark", required=True, help="tpch or tpcds")
    parser.add_argument("--title", required=True, help="Chart title")
    parser.add_argument("--output-dir", default=".", help="Directory for chart PNGs")
    args = parser.parse_args(argv)

    if len(args.filenames) != len(args.labels):
        parser.error("Number of filenames must match number of labels")

    compare(args.filenames, args.labels, args.benchmark, args.title, args.output_dir)


if __name__ == "__main__":
    main()

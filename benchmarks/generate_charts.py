#!/usr/bin/env python3
"""Generate peak memory and throughput charts from shuffle benchmark results."""

import csv
import matplotlib.pyplot as plt
import numpy as np

CSV_FILE = "/tmp/shuffle_bench_results_partitions/results.csv"
OUTPUT_DIR = "/tmp/shuffle_bench_results_partitions"

rows = []
with open(CSV_FILE) as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows.append(row)

labels = []
buffered_mem = []
immediate_mem = []
buffered_tp = []
immediate_tp = []

for row in rows:
    if row["mode"] == "buffered":
        labels.append(str(row["partitions"]))
        buffered_mem.append(int(row["peak_memory_bytes"]) / (1024**3))
        buffered_tp.append(int(row["throughput_rows_per_sec"]) / 1e6)

for row in rows:
    if row["mode"] == "immediate":
        immediate_mem.append(int(row["peak_memory_bytes"]) / (1024**3))
        immediate_tp.append(int(row["throughput_rows_per_sec"]) / 1e6)

x = np.arange(len(labels))
width = 0.35

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 12,
})

# Chart 1: Peak Memory
fig, ax = plt.subplots(figsize=(10, 6))
bars1 = ax.bar(x - width/2, buffered_mem, width, label="buffered", color="#4C78A8")
bars2 = ax.bar(x + width/2, immediate_mem, width, label="immediate", color="#E45756")
ax.set_xlabel("Output Partitions")
ax.set_ylabel("Peak RSS (GiB)")
ax.set_title("Shuffle Peak Memory Usage\n(4 GB memory limit, 100M rows, lz4, hash on cols 0,3)")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.bar_label(bars1, fmt="%.1f", padding=3, fontsize=9)
ax.bar_label(bars2, fmt="%.1f", padding=3, fontsize=9)
fig.tight_layout()
fig.savefig(f"{OUTPUT_DIR}/shuffle_peak_memory.png", dpi=150)
print(f"Saved {OUTPUT_DIR}/shuffle_peak_memory.png")

# Chart 2: Throughput
fig, ax = plt.subplots(figsize=(10, 6))
bars1 = ax.bar(x - width/2, buffered_tp, width, label="buffered", color="#4C78A8")
bars2 = ax.bar(x + width/2, immediate_tp, width, label="immediate", color="#E45756")
ax.set_xlabel("Output Partitions")
ax.set_ylabel("Throughput (M rows/s)")
ax.set_title("Shuffle Throughput\n(4 GB memory limit, 100M rows, lz4, hash on cols 0,3)")
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
ax.bar_label(bars1, fmt="%.2f", padding=3, fontsize=9)
ax.bar_label(bars2, fmt="%.2f", padding=3, fontsize=9)
fig.tight_layout()
fig.savefig(f"{OUTPUT_DIR}/shuffle_throughput.png", dpi=150)
print(f"Saved {OUTPUT_DIR}/shuffle_throughput.png")

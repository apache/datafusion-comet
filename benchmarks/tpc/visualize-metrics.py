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

import argparse
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


# Stable colors: driver=blue, executor-0/worker-1=orange, executor-1/worker-2=green
ENTITY_COLORS = {
    'driver': 'tab:blue',
    '0': 'tab:orange',
    '1': 'tab:green',
    '2': 'tab:red',
    '3': 'tab:purple',
    'worker-1': 'tab:orange',
    'worker-2': 'tab:green',
}

DEFAULT_COLORS = ['tab:orange', 'tab:green', 'tab:red', 'tab:purple', 'tab:brown', 'tab:pink']


def color_for(entity_id):
    """Return a stable color for the given entity identifier."""
    if entity_id in ENTITY_COLORS:
        return ENTITY_COLORS[entity_id]
    # Hash-based fallback for unknown entities
    idx = hash(entity_id) % len(DEFAULT_COLORS)
    return DEFAULT_COLORS[idx]


def auto_unit(max_bytes):
    """Pick MB or GB and return (divisor, label)."""
    if max_bytes > 1e9:
        return 1e9, 'GB'
    return 1e6, 'MB'


def load_jvm_metrics(path):
    """Load JVM metrics CSV and return data grouped by executor_id."""
    data = defaultdict(lambda: defaultdict(list))
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            eid = row['executor_id']
            for key, val in row.items():
                if key == 'executor_id':
                    data[eid][key].append(val)
                elif key == 'is_active':
                    data[eid][key].append(val == 'True')
                else:
                    data[eid][key].append(float(val))
    return data


def load_cgroup_metrics(path, label):
    """Load a cgroup metrics CSV and return dict with lists of values."""
    data = {'label': label}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return data
    timestamps = [float(r['timestamp_ms']) for r in rows]
    data['timestamp_ms'] = timestamps
    min_ts = min(timestamps)
    data['elapsed_secs'] = [(ts - min_ts) / 1000.0 for ts in timestamps]
    for key in ['memory_usage_bytes', 'memory_limit_bytes', 'rss_bytes', 'cache_bytes', 'swap_bytes']:
        if key in rows[0]:
            data[key] = [float(r[key]) for r in rows]
    return data


def generate_jvm_memory_usage(jvm_data, output_dir, title):
    """Chart 1: Peak-so-far JVM memory per executor over time (driver excluded).

    The Spark REST API only exposes monotonically-increasing peak counters
    (not current usage), so the lines show the running peak at each poll.
    """
    peak_series = [
        ('peak_JVMHeapMemory', 'Heap', 'solid'),
        ('peak_JVMOffHeapMemory', 'OffHeap', 'dashed'),
        ('peak_OffHeapExecutionMemory', 'OffHeapExec', 'dotted'),
    ]

    executors = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    if not executors:
        print('  Skipped jvm_memory_usage.png (no executor data)')
        return

    # Check whether there is any non-zero peak data
    all_values = []
    for series in executors.values():
        for field, _, _ in peak_series:
            all_values.extend(series.get(field, []))
    if not all_values or max(all_values) == 0:
        print('  Skipped jvm_memory_usage.png (all peak values are zero)')
        return

    divisor, unit = auto_unit(max(all_values))

    fig, ax = plt.subplots(figsize=(14, 6))
    for eid, series in sorted(executors.items()):
        c = color_for(eid)
        for field, label, ls in peak_series:
            vals = series.get(field, [])
            if vals and max(vals) > 0:
                ax.plot(series['elapsed_secs'],
                        [v / divisor for v in vals],
                        color=c, linestyle=ls, linewidth=1.5,
                        label=f'executor {eid} {label}')

    ax.set_title(f'{title} — JVM Peak Memory Over Time' if title else 'JVM Peak Memory Over Time')
    ax.set_xlabel('Elapsed Time (seconds)')
    ax.set_ylabel(f'Peak Memory ({unit})')
    ax.legend(fontsize=8)
    ax.yaxis.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'jvm_memory_usage.png'), format='png')
    plt.close(fig)
    print(f'  Created jvm_memory_usage.png')


def generate_jvm_peak_memory(jvm_data, output_dir, title):
    """Chart 2: Peak memory breakdown per executor, excluding driver (grouped bar)."""
    peak_fields = [
        'peak_JVMHeapMemory',
        'peak_JVMOffHeapMemory',
        'peak_OnHeapExecutionMemory',
        'peak_OffHeapExecutionMemory',
    ]

    executor_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}

    # Check if all peak values are zero — skip chart if so
    all_zero = True
    for eid, series in executor_data.items():
        for field in peak_fields:
            if field in series and max(series[field]) > 0:
                all_zero = False
                break
        if not all_zero:
            break
    if all_zero:
        print('  Skipped jvm_peak_memory.png (all peak values are zero)')
        return

    executors = sorted(executor_data.keys())
    # Use the max of each peak field per executor
    peak_values = {}
    all_vals = []
    for eid in executors:
        peak_values[eid] = {}
        for field in peak_fields:
            val = max(executor_data[eid].get(field, [0]))
            peak_values[eid][field] = val
            all_vals.append(val)

    divisor, unit = auto_unit(max(all_vals)) if all_vals else (1e6, 'MB')

    import numpy as np
    x = np.arange(len(executors))
    n_fields = len(peak_fields)
    bar_width = 0.8 / n_fields
    field_colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red']

    fig, ax = plt.subplots(figsize=(max(8, len(executors) * 2), 6))
    for i, field in enumerate(peak_fields):
        vals = [peak_values[eid][field] / divisor for eid in executors]
        short_label = field.replace('peak_', '')
        ax.bar(x + i * bar_width, vals, bar_width, label=short_label, color=field_colors[i])

    ax.set_title(f'{title} — JVM Peak Memory' if title else 'JVM Peak Memory')
    ax.set_xlabel('Executor')
    ax.set_ylabel(f'Peak Memory ({unit})')
    ax.set_xticks(x + bar_width * (n_fields - 1) / 2)
    ax.set_xticklabels([f'executor {eid}' for eid in executors])
    ax.legend(fontsize=8)
    ax.yaxis.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'jvm_peak_memory.png'), format='png')
    plt.close(fig)
    print(f'  Created jvm_peak_memory.png')


def _jvm_time_range(jvm_data, cgroup_datasets):
    """Compute the x-axis limit from JVM data using absolute timestamps.

    Returns the max elapsed time (in seconds, relative to the global time-zero
    across both data sources) that the JVM profiler was active, or None if
    absolute timestamps are not available.
    """
    exec_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    has_jvm_ts = any('timestamp_ms' in s for s in exec_data.values())
    has_cg_ts = any('timestamp_ms' in ds for ds in cgroup_datasets)
    if not (has_jvm_ts and has_cg_ts):
        return None
    all_abs = []
    for s in exec_data.values():
        all_abs.extend(s['timestamp_ms'])
    for ds in cgroup_datasets:
        all_abs.extend(ds.get('timestamp_ms', []))
    t_zero = min(all_abs)
    jvm_max = 0
    for s in exec_data.values():
        jvm_max = max(jvm_max, max(s['timestamp_ms']))
    return (jvm_max - t_zero) / 1000.0


def generate_cgroup_memory(cgroup_datasets, jvm_data, output_dir, title):
    """Chart 3: cgroup memory_usage_bytes (solid) and rss_bytes (dashed) per worker."""
    x_max = _jvm_time_range(jvm_data, cgroup_datasets)

    # Convert cgroup timestamps to the same shared time-zero as the combined chart
    exec_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    has_jvm_ts = any('timestamp_ms' in s for s in exec_data.values())
    has_cg_ts = any('timestamp_ms' in ds for ds in cgroup_datasets)
    if has_jvm_ts and has_cg_ts:
        all_abs = []
        for s in exec_data.values():
            all_abs.extend(s['timestamp_ms'])
        for ds in cgroup_datasets:
            all_abs.extend(ds.get('timestamp_ms', []))
        t_zero = min(all_abs)
        def elapsed_for(ds):
            return [(t - t_zero) / 1000.0 for t in ds['timestamp_ms']]
    else:
        def elapsed_for(ds):
            return ds.get('elapsed_secs', [])

    fig, ax = plt.subplots(figsize=(14, 6))

    all_values = []
    for ds in cgroup_datasets:
        all_values.extend(ds.get('memory_usage_bytes', []))
        all_values.extend(ds.get('rss_bytes', []))

    divisor, unit = auto_unit(max(all_values)) if all_values else (1e6, 'MB')

    for ds in cgroup_datasets:
        label = ds['label']
        c = color_for(label)
        elapsed = elapsed_for(ds)
        if 'memory_usage_bytes' in ds:
            ax.plot(elapsed,
                    [v / divisor for v in ds['memory_usage_bytes']],
                    color=c, linewidth=1.5, label=f'{label} usage')
        if 'rss_bytes' in ds:
            ax.plot(elapsed,
                    [v / divisor for v in ds['rss_bytes']],
                    color=c, linewidth=1.5, linestyle='--', label=f'{label} RSS')

    if x_max is not None:
        ax.set_xlim(left=0, right=x_max * 1.02)

    ax.set_title(f'{title} — Container Memory (cgroup)' if title else 'Container Memory (cgroup)')
    ax.set_xlabel('Elapsed Time (seconds)')
    ax.set_ylabel(f'Memory ({unit})')
    ax.legend(fontsize=8)
    ax.yaxis.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'cgroup_memory.png'), format='png')
    plt.close(fig)
    print(f'  Created cgroup_memory.png')


def generate_combined_memory(jvm_data, cgroup_datasets, cgroup_offset, output_dir, title):
    """Chart 4: Dual-axis — JVM memoryUsed per executor (left) + cgroup usage per worker (right).

    Aligns both data sources onto a shared elapsed-time axis.  When the JVM CSV
    contains ``timestamp_ms`` (absolute epoch), the script computes a common
    time-zero from the earliest timestamp across *both* sources.  Otherwise it
    falls back to ``elapsed_secs`` with the manual ``--cgroup-offset``.
    """
    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax2 = ax1.twinx()

    # --- Determine shared time-zero from absolute timestamps when available ---
    exec_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    has_jvm_ts = any('timestamp_ms' in series for series in exec_data.values())
    has_cg_ts = any('timestamp_ms' in ds for ds in cgroup_datasets)

    if has_jvm_ts and has_cg_ts:
        # Gather all absolute timestamps to find the global minimum
        all_abs = []
        for series in exec_data.values():
            all_abs.extend(series['timestamp_ms'])
        for ds in cgroup_datasets:
            all_abs.extend(ds.get('timestamp_ms', []))
        t_zero = min(all_abs)

        def jvm_elapsed(series):
            return [(t - t_zero) / 1000.0 for t in series['timestamp_ms']]

        def cg_elapsed(ds):
            return [(t - t_zero) / 1000.0 for t in ds['timestamp_ms']]
    else:
        # Fallback: use elapsed_secs with manual offset
        def jvm_elapsed(series):
            return series['elapsed_secs']

        def cg_elapsed(ds):
            elapsed = ds.get('elapsed_secs', [])
            if cgroup_offset != 0:
                return [t + cgroup_offset for t in elapsed]
            return elapsed

    # --- JVM: per-executor peak memory over time (driver excluded) ---
    executor_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    jvm_peak_fields = [
        ('peak_JVMHeapMemory', 'Heap', 'solid'),
        ('peak_OffHeapExecutionMemory', 'OffHeapExec', 'dotted'),
    ]
    all_jvm_vals = []
    for series in executor_data.values():
        for field, _, _ in jvm_peak_fields:
            all_jvm_vals.extend(series.get(field, []))
    all_cg_vals = []
    for ds in cgroup_datasets:
        all_cg_vals.extend(ds.get('memory_usage_bytes', []))

    jvm_div, jvm_unit = auto_unit(max(all_jvm_vals)) if all_jvm_vals and max(all_jvm_vals) > 0 else (1e6, 'MB')
    cg_div, cg_unit = auto_unit(max(all_cg_vals)) if all_cg_vals else (1e6, 'MB')

    for eid, series in sorted(executor_data.items()):
        c = color_for(eid)
        for field, label, ls in jvm_peak_fields:
            vals = series.get(field, [])
            if vals and max(vals) > 0:
                ax1.plot(jvm_elapsed(series),
                         [v / jvm_div for v in vals],
                         color=c, linestyle=ls, linewidth=1.5,
                         label=f'executor {eid} {label}')

    ax1.set_xlabel('Elapsed Time (seconds)')
    ax1.set_ylabel(f'JVM Peak Memory ({jvm_unit})')
    ax1.tick_params(axis='y')

    # --- Cgroup: usage per worker ---
    for ds in cgroup_datasets:
        label = ds['label']
        c = color_for(label)
        if 'memory_usage_bytes' in ds:
            ax2.plot(cg_elapsed(ds),
                     [v / cg_div for v in ds['memory_usage_bytes']],
                     color=c, linewidth=1.5, linestyle='--', label=f'{label} cgroup usage')

    ax2.set_ylabel(f'Container Memory ({cg_unit})')
    ax2.tick_params(axis='y')

    # Truncate x-axis to JVM time range
    x_max = _jvm_time_range(jvm_data, cgroup_datasets)
    if x_max is not None:
        ax1.set_xlim(left=0, right=x_max * 1.02)

    # Combine legends from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8, loc='upper left')

    ax1.set_title(f'{title} — Combined Memory Overview' if title else 'Combined Memory Overview')
    ax1.yaxis.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'combined_memory.png'), format='png')
    plt.close(fig)
    print(f'  Created combined_memory.png')


def main():
    parser = argparse.ArgumentParser(
        description='Visualize TPC benchmark memory metrics as time-series charts.')
    parser.add_argument('--jvm-metrics', type=str, required=True,
                        help='Path to JVM metrics CSV (comet-tpch-metrics.csv)')
    parser.add_argument('--cgroup-metrics', type=str, nargs='+', default=None,
                        help='Paths to cgroup metrics CSVs (container-metrics-worker-*.csv)')
    parser.add_argument('--output-dir', type=str, default='.',
                        help='Directory to write chart PNGs (default: current directory)')
    parser.add_argument('--title', type=str, default='',
                        help='Title prefix for charts')
    parser.add_argument('--cgroup-offset', type=float, default=0,
                        help='Seconds to shift cgroup timestamps for alignment with JVM data (default: 0)')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f'Loading JVM metrics from {args.jvm_metrics}')
    jvm_data = load_jvm_metrics(args.jvm_metrics)
    print(f'  Found executors: {", ".join(sorted(jvm_data.keys()))}')

    # Compute the JVM time window so cgroup charts can be truncated to match.
    jvm_max_elapsed = 0
    for eid, series in jvm_data.items():
        if eid != 'driver' and series['elapsed_secs']:
            jvm_max_elapsed = max(jvm_max_elapsed, max(series['elapsed_secs']))

    generate_jvm_memory_usage(jvm_data, args.output_dir, args.title)
    generate_jvm_peak_memory(jvm_data, args.output_dir, args.title)

    if args.cgroup_metrics:
        cgroup_datasets = []
        for i, path in enumerate(args.cgroup_metrics, start=1):
            label = f'worker-{i}'
            print(f'Loading cgroup metrics from {path} (as {label})')
            cgroup_datasets.append(load_cgroup_metrics(path, label))

        generate_cgroup_memory(cgroup_datasets, jvm_data,
                               args.output_dir, args.title)
        generate_combined_memory(jvm_data, cgroup_datasets, args.cgroup_offset,
                                 args.output_dir, args.title)

    print(f'Done. Charts written to {args.output_dir}')


if __name__ == '__main__':
    main()

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

PEAK_SERIES = [
    ('peak_JVMHeapMemory', 'Heap', 'solid', 'tab:blue'),
    ('peak_JVMOffHeapMemory', 'OffHeap', 'solid', 'tab:orange'),
    ('peak_OffHeapExecutionMemory', 'OffHeapExec', 'solid', 'tab:red'),
]

PEAK_BAR_FIELDS = [
    'peak_JVMHeapMemory',
    'peak_JVMOffHeapMemory',
    'peak_OnHeapExecutionMemory',
    'peak_OffHeapExecutionMemory',
]


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


def _shared_time_zero(jvm_data, cgroup_datasets):
    """Compute the global time-zero from absolute timestamps across both sources.

    Returns ``t_zero`` (ms) or ``None`` if absolute timestamps are unavailable.
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
    return min(all_abs)


def _jvm_time_range(jvm_data, cgroup_datasets):
    """Compute the x-axis limits from JVM data using absolute timestamps.

    Returns ``(x_min, x_max)`` — the elapsed-time window (in seconds, relative
    to the global time-zero across both data sources) during which the JVM
    profiler was active, or ``None`` if absolute timestamps are not available.
    """
    t_zero = _shared_time_zero(jvm_data, cgroup_datasets)
    if t_zero is None:
        return None
    exec_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    jvm_min = float('inf')
    jvm_max = 0
    for s in exec_data.values():
        jvm_min = min(jvm_min, min(s['timestamp_ms']))
        jvm_max = max(jvm_max, max(s['timestamp_ms']))
    return ((jvm_min - t_zero) / 1000.0, (jvm_max - t_zero) / 1000.0)


def generate_jvm_memory_usage(jvm_data, output_dir, title):
    """Generate one JVM peak-memory time-series chart per executor (driver excluded)."""
    executors = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    if not executors:
        print('  Skipped jvm_memory (no executor data)')
        return

    for eid, series in sorted(executors.items()):
        all_values = []
        for field, _, _, _ in PEAK_SERIES:
            all_values.extend(series.get(field, []))
        if not all_values or max(all_values) == 0:
            print(f'  Skipped jvm_memory_executor_{eid}.png (all peak values are zero)')
            continue

        divisor, unit = auto_unit(max(all_values))

        fig, ax = plt.subplots(figsize=(14, 6))
        for field, label, ls, color in PEAK_SERIES:
            vals = series.get(field, [])
            if vals and max(vals) > 0:
                ax.plot(series['elapsed_secs'],
                        [v / divisor for v in vals],
                        color=color, linestyle=ls, linewidth=1.5,
                        label=label)

        suffix = f' — Executor {eid} JVM Peak Memory' if title else f'Executor {eid} JVM Peak Memory'
        ax.set_title(f'{title}{suffix}')
        ax.set_xlabel('Elapsed Time (seconds)')
        ax.set_ylabel(f'Peak Memory ({unit})')
        ax.legend(fontsize=8)
        ax.yaxis.grid(True)
        plt.tight_layout()
        fname = f'jvm_memory_executor_{eid}.png'
        plt.savefig(os.path.join(output_dir, fname), format='png')
        plt.close(fig)
        print(f'  Created {fname}')


def generate_jvm_peak_memory(jvm_data, output_dir, title):
    """Peak memory breakdown per executor, excluding driver (grouped bar)."""
    executor_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}

    # Check if all peak values are zero — skip chart if so
    all_zero = True
    for eid, series in executor_data.items():
        for field in PEAK_BAR_FIELDS:
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
        for field in PEAK_BAR_FIELDS:
            val = max(executor_data[eid].get(field, [0]))
            peak_values[eid][field] = val
            all_vals.append(val)

    divisor, unit = auto_unit(max(all_vals)) if all_vals else (1e6, 'MB')

    import numpy as np
    x = np.arange(len(executors))
    n_fields = len(PEAK_BAR_FIELDS)
    bar_width = 0.8 / n_fields
    field_colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red']

    fig, ax = plt.subplots(figsize=(max(8, len(executors) * 2), 6))
    for i, field in enumerate(PEAK_BAR_FIELDS):
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


def generate_cgroup_memory(cgroup_datasets, jvm_data, output_dir, title):
    """Generate one cgroup memory chart per worker."""
    jvm_range = _jvm_time_range(jvm_data, cgroup_datasets)
    t_zero = _shared_time_zero(jvm_data, cgroup_datasets)

    for ds in cgroup_datasets:
        label = ds['label']
        c = color_for(label)

        if t_zero is not None and 'timestamp_ms' in ds:
            elapsed = [(t - t_zero) / 1000.0 for t in ds['timestamp_ms']]
        else:
            elapsed = ds.get('elapsed_secs', [])

        all_values = []
        all_values.extend(ds.get('memory_usage_bytes', []))
        all_values.extend(ds.get('rss_bytes', []))
        if not all_values:
            continue

        divisor, unit = auto_unit(max(all_values))

        fig, ax = plt.subplots(figsize=(14, 6))
        if 'memory_usage_bytes' in ds:
            ax.plot(elapsed,
                    [v / divisor for v in ds['memory_usage_bytes']],
                    color='tab:blue', linewidth=1.5, label='usage')
        if 'rss_bytes' in ds:
            ax.plot(elapsed,
                    [v / divisor for v in ds['rss_bytes']],
                    color='tab:orange', linewidth=1.5, label='RSS')

        if jvm_range is not None:
            x_min, x_max = jvm_range
            ax.set_xlim(left=x_min, right=x_max * 1.02)

        suffix = f' — {label} Container Memory (cgroup)'
        ax.set_title(f'{title}{suffix}' if title else suffix.lstrip(' — '))
        ax.set_xlabel('Elapsed Time (seconds)')
        ax.set_ylabel(f'Memory ({unit})')
        ax.legend(fontsize=8)
        ax.yaxis.grid(True)
        plt.tight_layout()
        fname = f'cgroup_memory_{label}.png'
        plt.savefig(os.path.join(output_dir, fname), format='png')
        plt.close(fig)
        print(f'  Created {fname}')


def generate_combined_memory(jvm_data, cgroup_datasets, cgroup_offset, output_dir, title):
    """Generate one combined log-scale chart per worker, pairing executor N with worker-(N+1)."""
    exec_data = {eid: s for eid, s in jvm_data.items() if eid != 'driver'}
    t_zero = _shared_time_zero(jvm_data, cgroup_datasets)
    jvm_range = _jvm_time_range(jvm_data, cgroup_datasets)

    # Build elapsed-time helpers
    if t_zero is not None:
        def jvm_elapsed(series):
            return [(t - t_zero) / 1000.0 for t in series['timestamp_ms']]
        def cg_elapsed(ds):
            return [(t - t_zero) / 1000.0 for t in ds['timestamp_ms']]
    else:
        def jvm_elapsed(series):
            return series['elapsed_secs']
        def cg_elapsed(ds):
            elapsed = ds.get('elapsed_secs', [])
            if cgroup_offset != 0:
                return [t + cgroup_offset for t in elapsed]
            return elapsed

    # Pair executor IDs with cgroup datasets by index:
    # sorted executors ['0','1'] map to cgroup_datasets [worker-1, worker-2]
    sorted_eids = sorted(exec_data.keys())

    for idx, ds in enumerate(cgroup_datasets):
        label = ds['label']
        eid = sorted_eids[idx] if idx < len(sorted_eids) else None

        fig, ax = plt.subplots(figsize=(14, 6))

        # All series plotted in GB on a single log-scale axis
        divisor = 1e9
        unit = 'GB'

        # --- JVM: peak memory for this executor ---
        if eid is not None:
            series = exec_data[eid]
            for field, flabel, ls, color in PEAK_SERIES:
                vals = series.get(field, [])
                if vals and max(vals) > 0:
                    ax.plot(jvm_elapsed(series),
                            [v / divisor for v in vals],
                            color=color, linestyle=ls, linewidth=1.5,
                            label=f'JVM {flabel}')

        # --- Cgroup: usage for this worker ---
        if 'memory_usage_bytes' in ds:
            ax.plot(cg_elapsed(ds),
                    [v / divisor for v in ds['memory_usage_bytes']],
                    color='tab:purple', linewidth=1.5, linestyle='--', label='cgroup usage')
        if 'rss_bytes' in ds:
            ax.plot(cg_elapsed(ds),
                    [v / divisor for v in ds['rss_bytes']],
                    color='tab:brown', linewidth=1, linestyle='--', label='cgroup RSS')

        ax.set_yscale('log')
        ax.set_xlabel('Elapsed Time (seconds)')
        ax.set_ylabel(f'Memory ({unit}, log scale)')

        if jvm_range is not None:
            x_min, x_max = jvm_range
            ax.set_xlim(left=x_min, right=x_max * 1.02)

        ax.legend(fontsize=8, loc='upper left')

        eid_label = f'executor {eid} / {label}' if eid else label
        suffix = f' — {eid_label} Combined Memory'
        ax.set_title(f'{title}{suffix}' if title else suffix.lstrip(' — '))
        ax.yaxis.grid(True, which='both')
        plt.tight_layout()
        fname = f'combined_memory_{label}.png'
        plt.savefig(os.path.join(output_dir, fname), format='png')
        plt.close(fig)
        print(f'  Created {fname}')


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

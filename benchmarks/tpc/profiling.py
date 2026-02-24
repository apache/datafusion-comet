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
Level 1 profiling hooks: JVM metrics via the Spark REST API.

Polls ``/api/v1/applications/{appId}/executors`` at a configurable interval
and records executor memory metrics as a time-series CSV alongside the
benchmark results.

Usage::

    profiler = SparkMetricsProfiler(spark, interval_secs=2)
    profiler.start()
    # ... run benchmark ...
    profiler.stop()
    profiler.write_csv("/path/to/output/metrics.csv")
"""

import csv
import glob
import os
import threading
import time
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

try:
    from urllib.request import urlopen
    import json as _json

    def _fetch_json(url: str) -> Any:
        with urlopen(url, timeout=5) as resp:
            return _json.loads(resp.read().decode())
except ImportError:
    _fetch_json = None  # type: ignore[assignment]


# Metrics we extract per executor from the REST API response
_EXECUTOR_METRICS = [
    "memoryUsed",
    "maxMemory",
    "totalOnHeapStorageMemory",
    "usedOnHeapStorageMemory",
    "totalOffHeapStorageMemory",
    "usedOffHeapStorageMemory",
]

# Metrics nested under peakMemoryMetrics (if available)
_PEAK_MEMORY_METRICS = [
    "JVMHeapMemory",
    "JVMOffHeapMemory",
    "OnHeapExecutionMemory",
    "OffHeapExecutionMemory",
    "OnHeapStorageMemory",
    "OffHeapStorageMemory",
    "OnHeapUnifiedMemory",
    "OffHeapUnifiedMemory",
    "ProcessTreeJVMRSSMemory",
]


class SparkMetricsProfiler:
    """Periodically polls executor metrics from the Spark REST API."""

    def __init__(
        self,
        spark: SparkSession,
        interval_secs: float = 2.0,
    ):
        self._spark = spark
        self._interval = interval_secs
        self._samples: List[Dict[str, Any]] = []
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_time: float = 0.0
        self._stop_time: float = 0.0

    @property
    def samples(self) -> List[Dict[str, Any]]:
        """Return collected samples (each is a flat dict)."""
        return list(self._samples)

    def _ui_url(self) -> Optional[str]:
        """Return the Spark UI base URL, or None if unavailable."""
        url = self._spark.sparkContext.uiWebUrl
        if url:
            return url.rstrip("/")
        return None

    def _app_id(self) -> str:
        return self._spark.sparkContext.applicationId

    def _poll_once(self) -> None:
        """Fetch executor metrics and append a timestamped sample."""
        base = self._ui_url()
        if base is None or _fetch_json is None:
            return

        url = f"{base}/api/v1/applications/{self._app_id()}/executors"
        try:
            executors = _fetch_json(url)
        except Exception:
            return

        now = time.time()
        elapsed = now - self._start_time
        timestamp_ms = int(now * 1000)
        for exc in executors:
            row: Dict[str, Any] = {
                "timestamp_ms": timestamp_ms,
                "elapsed_secs": round(elapsed, 2),
                "executor_id": exc.get("id", ""),
                "is_active": exc.get("isActive", True),
            }
            for key in _EXECUTOR_METRICS:
                row[key] = exc.get(key, 0)

            peak = exc.get("peakMemoryMetrics", {})
            for key in _PEAK_MEMORY_METRICS:
                row[f"peak_{key}"] = peak.get(key, 0)

            self._samples.append(row)

    def _run(self) -> None:
        """Background polling loop."""
        while not self._stop_event.is_set():
            self._poll_once()
            self._stop_event.wait(self._interval)

    def start(self) -> None:
        """Start background polling thread."""
        if self._thread is not None:
            return
        self._start_time = time.time()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="spark-metrics-profiler", daemon=True
        )
        self._thread.start()
        print(
            f"Profiler started (interval={self._interval}s, "
            f"ui={self._ui_url()})"
        )

    def stop(self) -> None:
        """Stop the polling thread and collect a final sample."""
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join(timeout=self._interval + 2)
        self._thread = None
        # One last poll to capture final state
        self._poll_once()
        self._stop_time = time.time()
        print(f"Profiler stopped ({len(self._samples)} samples collected)")

    def write_csv(self, path: str) -> str:
        """Write collected samples to a CSV file.  Returns the path."""
        if not self._samples:
            print("Profiler: no samples to write")
            return path

        fieldnames = list(self._samples[0].keys())
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in self._samples:
                writer.writerow(row)
        print(f"Profiler: wrote {len(self._samples)} samples to {path}")
        return path

    def snapshot_cgroup_metrics(
        self, output_dir: str, name: str, benchmark: str
    ) -> List[str]:
        """Filter cgroup CSVs to the profiling time window and write snapshots.

        Looks for ``container-metrics-*.csv`` in *output_dir*, keeps only
        rows whose ``timestamp_ms`` falls within the profiler's start/stop
        window, and writes each filtered file as
        ``{name}-{benchmark}-container-metrics-{label}.csv``.

        Returns the list of snapshot file paths written.
        """
        start_ms = int(self._start_time * 1000)
        stop_ms = int(self._stop_time * 1000) if self._stop_time else int(
            time.time() * 1000
        )

        source_files = sorted(
            glob.glob(os.path.join(output_dir, "container-metrics-*.csv"))
        )
        if not source_files:
            print("Profiler: no container-metrics CSVs found to snapshot")
            return []

        written: List[str] = []
        for src in source_files:
            # Extract label, e.g. "spark-worker-1" from
            # "container-metrics-spark-worker-1.csv"
            basename = os.path.basename(src)
            label = basename.replace("container-metrics-", "").replace(
                ".csv", ""
            )
            dest = os.path.join(
                output_dir,
                f"{name}-{benchmark}-container-metrics-{label}.csv",
            )

            with open(src, "r", newline="") as fin:
                reader = csv.DictReader(fin)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    continue
                rows = [
                    row
                    for row in reader
                    if start_ms <= int(row["timestamp_ms"]) <= stop_ms
                ]

            with open(dest, "w", newline="") as fout:
                writer = csv.DictWriter(fout, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            print(
                f"Profiler: snapshot {len(rows)} cgroup rows -> {dest}"
            )
            written.append(dest)

        return written

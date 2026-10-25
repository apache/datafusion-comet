#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run native Iceberg Bloom off/on in separate JVMs, reporting timing and process peak RSS."""

import argparse
import filecmp
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import run as micro


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-profile", default="spark-4.1")
    parser.add_argument("--rows", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--heap", default=micro.DEFAULT_HEAP)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--allow-debug",
        action="store_true",
        help="smoke checks only, not performance results",
    )
    args = parser.parse_args()
    if sys.platform not in ("darwin", "linux"):
        parser.error("peak-RSS collection supports Linux and macOS")
    if not 8 <= args.rows <= 2147483647 or args.iterations < 1:
        parser.error("rows must be in [8, 2147483647], and iterations must be positive")
    repo = Path(__file__).resolve().parents[2]
    profile = "-P" + args.spark_profile
    env = micro.base_env(repo)
    micro.require_supported_jdk(env)
    if not args.allow_debug:
        library = "libcomet.dylib" if sys.platform == "darwin" else "libcomet.so"
        release = repo / "native/target/release" / library
        packaged = (repo / "spark/target/classes/org/apache/comet").rglob(library)
        if not release.is_file() or not any(
            filecmp.cmp(release, path, shallow=False) for path in packaged
        ):
            parser.error(
                "build/install release artifacts first with make release PROFILES="
                + profile
            )
    bench = micro.benchmark_args(repo, env, profile, args.heap)
    env["MAVEN_OPTS"] = bench["MAVEN_OPTS"]
    # Each arm's result table stays in its own log instead of overwriting the suite result file.
    env.pop("SPARK_GENERATE_BENCHMARK_FILES", None)
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="comet-iceberg-bloom-"))
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    results = []
    for arm in ("off", "on"):
        log = output / ("bloom-" + arm + ".log")
        command = micro.suite_command(
            "CometIcebergWriteBenchmark", bench["MVN_ARGS"], profile
        )
        command.append(f"-Dexec.args=bloom-{arm} {args.rows} {args.iterations}")
        started = time.monotonic()
        with log.open("w") as stream:
            process = subprocess.Popen(
                command,
                cwd=repo / "spark",
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            try:
                _, status, usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)
            finally:
                if process.returncode is None:
                    process.terminate()
                    process.wait()
        if process.returncode:
            raise SystemExit(f"Bloom {arm} failed; see {log}")
        # wait4 returns bytes on macOS and KiB on Linux, independently for each child JVM.
        peak_bytes = int(usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024))
        label = "Native Bloom " + arm
        timing = [
            line.strip()
            for line in log.read_text().splitlines()
            if line.strip().startswith(label)
        ]
        if not timing:
            raise SystemExit(f"Bloom {arm} produced no benchmark timing row; see {log}")
        fields = timing[-1][len(label) :].split()
        try:
            best_ms, average_ms, stdev_ms = map(float, fields[:3])
        except ValueError:
            raise SystemExit(f"Cannot parse Spark benchmark timing columns; see {log}")
        results.append(
            {
                "bloom": arm,
                "write_best_ms": best_ms,
                "write_average_ms": average_ms,
                "write_stdev_ms": stdev_ms,
                "peak_rss_bytes": peak_bytes,
                "timing_row": timing[-1],
                "process_seconds": round(time.monotonic() - started, 3),
                "log": str(log),
            }
        )
        print(
            f"Bloom {arm}: write average {average_ms:.1f} ms "
            f"(best {best_ms:.1f}, stdev {stdev_ms:.1f})",
            flush=True,
        )
        print(f"Whole-process peak RSS: {peak_bytes / 1024**2:.1f} MiB", flush=True)
    summary = {
        "spark_profile": args.spark_profile,
        "rows": args.rows,
        "minimum_iterations": args.iterations,
        "heap": args.heap,
        "smoke_only": args.allow_debug,
        "platform": platform.platform(),
        "git": micro.git_info(repo),
        "results": results,
        "rss_scope": "Whole JVM process, including Maven, source generation, validation and warmup; not per-write allocation",
    }
    path = output / "summary.json"
    path.write_text(json.dumps(summary, indent=2) + "\n")
    print("Results: " + str(path), flush=True)


if __name__ == "__main__":
    main()

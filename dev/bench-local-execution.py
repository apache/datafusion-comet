#!/usr/bin/env python3
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

"""Manual local-mode benchmark launcher; see local-execution-benchmark.md."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["prepare", "coverage", "spark", "comet", "local"])
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=500000)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--library", choices=["release", "debug"], default="release")
    parser.add_argument("--schema-mode", choices=["infer", "explicit"], default="infer",
                        help="Use inferred or declared schemas for the synthetic timing fixtures")
    parser.add_argument("--jfr", action="store_true", help="Record a diagnostic JVM profile; do not compare its timings")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if args.mode in ("spark", "comet", "local") and args.library != "release":
        parser.error("Timing comparisons require an optimized release native library")
    report = root / "spark/target/surefire-reports/TEST-org.apache.comet.local.CometLocalExecutionSuite.xml"
    properties = {p.get("name"): p.get("value") for p in ET.parse(report).findall("./properties/property")}
    classpath = properties["java.class.path"]
    if "spark-sql_2.13/4.1." not in classpath:
        parser.error("Run the Spark 4.1 local suite first to obtain its test classpath")
    java = Path(os.environ.get("JAVA_HOME", properties["java.home"])) / "bin/java"
    library = root / "native/target" / args.library
    artifact = library / ("libcomet.dylib" if platform.system() == "Darwin" else "libcomet.so")
    if not artifact.exists():
        parser.error(f"Build the native library first: {artifact}")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / f"{args.mode}.log").exists():
        parser.error("Use a fresh output directory or a mode not previously run there")
    scratch = output / f"{args.mode}-tmp"
    scratch.mkdir()
    spark_scratch = scratch / "spark"
    native_scratch = scratch / "native"
    spark_scratch.mkdir()
    native_scratch.mkdir()
    java_scratch = scratch / "java"
    java_scratch.mkdir()
    pom = ET.parse(root / "pom.xml")
    opens = pom.find(".//{*}extraJavaTestArgs").text.split()
    command = [str(java), "-Xms512m", "-Xmx2g", *opens,
               "-Dtest.appender=console", f"-Djava.library.path={library}", f"-Djava.io.tmpdir={java_scratch}",
               f"-Dspark.local.dir={spark_scratch}", "-cp", classpath,
               "org.apache.comet.local.CometLocalExecutionBenchmark", args.mode,
               str(args.data.resolve()), str(output), str(args.rows), str(args.repetitions), args.schema_mode]
    if args.jfr:
        recording = output / f"{args.mode}.jfr"
        command[1:1] = ["-XX:FlightRecorderOptions=stackdepth=256",
                        f"-XX:StartFlightRecording=filename={recording},settings=profile,dumponexit=true"]
    env = {**os.environ, "COMET_WORKER_THREADS": "4", "TMPDIR": str(native_scratch),
           "COMET_CONF_DIR": str(root / "conf")}
    metadata = {"mode": args.mode, "rows": args.rows, "repetitions": args.repetitions,
                "host": platform.platform(), "java": str(java), "heap": "2g", "cores": 4,
                "schema_mode": args.schema_mode, "jfr": args.jfr,
                "native_profile": args.library, "library_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                "sampling_seconds": 0.1, "native_temp": str(native_scratch), "spark_temp": str(spark_scratch)}
    (output / f"{args.mode}-metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

    def disk_bytes(directory):
        total = 0
        # Spark can remove whole spill directories between samples.
        for parent, _, files in os.walk(directory):
            for name in files:
                try:
                    total += (Path(parent) / name).stat().st_size
                except FileNotFoundError:
                    pass
        return total

    with (output / f"{args.mode}.log").open("w") as log, (output / f"{args.mode}-samples.csv").open("w") as samples:
        writer = csv.writer(samples, lineterminator="\n")
        writer.writerow(["elapsed_s", "phase", "rss_kib", "native_temp_bytes", "spark_temp_bytes"])
        start = time.monotonic()
        process = subprocess.Popen(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            while process.poll() is None:
                result = subprocess.run(["ps", "-o", "rss=", "-p", str(process.pid)],
                                        capture_output=True, text=True, check=False)
                try:
                    rss = int(result.stdout.strip())
                except ValueError:
                    rss = ""
                phase_file = output / f"{args.mode}.phase"
                phase = phase_file.read_text().strip() if phase_file.exists() else "startup"
                writer.writerow([time.monotonic() - start, phase, rss,
                                 disk_bytes(native_scratch), disk_bytes(spark_scratch)])
                samples.flush()
                time.sleep(0.1)
        except BaseException:
            process.terminate()
            process.wait(timeout=30)
            raise
        if process.returncode:
            raise SystemExit(f"{args.mode} failed ({process.returncode}); inspect {output / (args.mode + '.log')}")
    if args.mode in ("spark", "comet", "local"):
        signatures = {}
        for mode in ("spark", "comet", "local"):
            path = output / f"{mode}.csv"
            if path.exists():
                with path.open() as handle:
                    for row in csv.DictReader(handle):
                        signature = (row["rows"], row["sha256"])
                        previous = signatures.setdefault(row["query"], signature)
                        if previous != signature:
                            raise SystemExit(f"Result mismatch: {mode}/{row['query']}")
        print("Result digests agree across all available runs (including warmups)")
    print(f"{args.mode}: completed; results in {output}")


if __name__ == "__main__":
    main()

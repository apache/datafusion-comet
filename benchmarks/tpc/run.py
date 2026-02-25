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

"""Consolidated TPC benchmark runner for Spark-based engines.

Usage:
    python3 run.py --engine comet --benchmark tpch
    python3 run.py --engine comet --benchmark tpcds --iterations 3
    python3 run.py --engine comet-iceberg --benchmark tpch
    python3 run.py --engine comet --benchmark tpch --dry-run
    python3 run.py --engine spark --benchmark tpch --no-restart
"""

import argparse
import os
import re
import subprocess
import sys

# ---------------------------------------------------------------------------
# TOML loading – prefer stdlib tomllib (3.11+), else minimal fallback
# ---------------------------------------------------------------------------

try:
    import tomllib  # Python 3.11+

    def load_toml(path):
        with open(path, "rb") as f:
            return tomllib.load(f)

except ModuleNotFoundError:

    def _parse_toml(text):
        """Minimal TOML parser supporting tables, quoted-key strings, plain
        strings, arrays of strings, booleans, and comments.  Sufficient for
        the engine config files used by this runner."""
        root = {}
        current = root
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Table header: [env.defaults] or [spark_conf]
            m = re.match(r"^\[([^\]]+)\]$", line)
            if m:
                keys = m.group(1).split(".")
                current = root
                for k in keys:
                    current = current.setdefault(k, {})
                continue
            # Key = value
            m = re.match(r'^("(?:[^"\\]|\\.)*"|[A-Za-z0-9_.]+)\s*=\s*(.+)$', line)
            if not m:
                continue
            raw_key, raw_val = m.group(1), m.group(2).strip()
            key = raw_key.strip('"')
            val = _parse_value(raw_val)
            current[key] = val
        return root

    def _parse_value(raw):
        if raw == "true":
            return True
        if raw == "false":
            return False
        if raw.startswith('"') and raw.endswith('"'):
            return raw[1:-1]
        if raw.startswith("["):
            # Simple array of strings
            items = []
            for m in re.finditer(r'"((?:[^"\\]|\\.)*)"', raw):
                items.append(m.group(1))
            return items
        if raw.startswith("{"):
            # Inline table: { KEY = "VAL", ... }
            tbl = {}
            for m in re.finditer(r'([A-Za-z0-9_]+)\s*=\s*"((?:[^"\\]|\\.)*)"', raw):
                tbl[m.group(1)] = m.group(2)
            return tbl
        return raw

    def load_toml(path):
        with open(path, "r") as f:
            return _parse_toml(f.read())


# ---------------------------------------------------------------------------
# Common Spark configuration (shared across all engines)
# ---------------------------------------------------------------------------

COMMON_SPARK_CONF = {
    "spark.driver.memory": "8G",
    "spark.executor.memory": "16g",
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "16g",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": os.environ.get("SPARK_EVENT_LOG_DIR", "/tmp/spark-events"),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
}

# ---------------------------------------------------------------------------
# Benchmark profiles
# ---------------------------------------------------------------------------

BENCHMARK_PROFILES = {
    "tpch": {
        "executor_instances": "2",
        "executor_cores": "8",
        "max_cores": "16",
        "data_env": "TPCH_DATA",
        "format": "parquet",
    },
    "tpcds": {
        "executor_instances": "2",
        "executor_cores": "8",
        "max_cores": "16",
        "data_env": "TPCDS_DATA",
        "format": None,  # omit --format for TPC-DS
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def resolve_env(value):
    """Expand $VAR and ${VAR} references using os.environ."""
    if not isinstance(value, str):
        return value
    return re.sub(
        r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)",
        lambda m: os.environ.get(m.group(1) or m.group(2), ""),
        value,
    )


def resolve_env_in_list(lst):
    return [resolve_env(v) for v in lst]


def load_engine_config(engine_name):
    """Load and return the TOML config for the given engine."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "engines", f"{engine_name}.toml")
    if not os.path.exists(config_path):
        available = sorted(
            f.removesuffix(".toml")
            for f in os.listdir(os.path.join(script_dir, "engines"))
            if f.endswith(".toml")
        )
        print(f"Error: Unknown engine '{engine_name}'", file=sys.stderr)
        print(f"Available engines: {', '.join(available)}", file=sys.stderr)
        sys.exit(1)
    return load_toml(config_path)


def apply_env_defaults(config):
    """Set environment variable defaults from [env.defaults]."""
    defaults = config.get("env", {}).get("defaults", {})
    for key, val in defaults.items():
        if key not in os.environ:
            os.environ[key] = val


def apply_env_exports(config):
    """Export environment variables from [env.exports]."""
    exports = config.get("env", {}).get("exports", {})
    for key, val in exports.items():
        os.environ[key] = val


def check_required_env(config):
    """Validate that required environment variables are set."""
    required = config.get("env", {}).get("required", [])
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(
            f"Error: Required environment variable(s) not set: {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)


def check_common_env():
    """Validate SPARK_HOME and SPARK_MASTER are set."""
    for var in ("SPARK_HOME", "SPARK_MASTER"):
        if not os.environ.get(var):
            print(f"Error: {var} is not set", file=sys.stderr)
            sys.exit(1)


def check_benchmark_env(config, benchmark):
    """Validate benchmark-specific environment variables."""
    profile = BENCHMARK_PROFILES[benchmark]
    use_iceberg = config.get("tpcbench_args", {}).get("use_iceberg", False)

    required = []
    if not use_iceberg:
        required.append(profile["data_env"])

    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(
            f"Error: Required environment variable(s) not set for "
            f"{benchmark}: {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Default ICEBERG_DATABASE to the benchmark name if not already set
    if use_iceberg and not os.environ.get("ICEBERG_DATABASE"):
        os.environ["ICEBERG_DATABASE"] = benchmark


def build_spark_submit_cmd(config, benchmark, args):
    """Build the spark-submit command list."""
    spark_home = os.environ["SPARK_HOME"]
    spark_master = os.environ["SPARK_MASTER"]
    profile = BENCHMARK_PROFILES[benchmark]

    cmd = [os.path.join(spark_home, "bin", "spark-submit")]
    cmd += ["--master", spark_master]

    # --jars
    jars = config.get("spark_submit", {}).get("jars", [])
    if jars:
        cmd += ["--jars", ",".join(resolve_env_in_list(jars))]

    # --driver-class-path
    driver_cp = config.get("spark_submit", {}).get("driver_class_path", [])
    if driver_cp:
        cmd += ["--driver-class-path", ":".join(resolve_env_in_list(driver_cp))]

    # Merge spark confs: common + benchmark profile + engine overrides
    conf = dict(COMMON_SPARK_CONF)
    conf["spark.executor.instances"] = profile["executor_instances"]
    conf["spark.executor.cores"] = profile["executor_cores"]
    conf["spark.cores.max"] = profile["max_cores"]

    engine_conf = config.get("spark_conf", {})
    for key, val in engine_conf.items():
        if isinstance(val, bool):
            val = "true" if val else "false"
        conf[resolve_env(key)] = resolve_env(str(val))

    for key, val in sorted(conf.items()):
        cmd += ["--conf", f"{key}={val}"]

    # tpcbench.py path
    cmd.append("tpcbench.py")

    # tpcbench args
    engine_name = config.get("engine", {}).get("name", args.engine)
    cmd += ["--name", engine_name]
    cmd += ["--benchmark", benchmark]

    use_iceberg = config.get("tpcbench_args", {}).get("use_iceberg", False)
    if use_iceberg:
        cmd += ["--catalog", resolve_env("${ICEBERG_CATALOG}")]
        cmd += ["--database", resolve_env("${ICEBERG_DATABASE}")]
    else:
        data_var = profile["data_env"]
        data_val = os.environ.get(data_var, "")
        cmd += ["--data", data_val]

    cmd += ["--output", args.output]
    cmd += ["--iterations", str(args.iterations)]

    if args.query is not None:
        cmd += ["--query", str(args.query)]

    if profile["format"] and not use_iceberg:
        cmd += ["--format", profile["format"]]

    if args.profile:
        cmd += ["--profile"]
        cmd += ["--profile-interval", str(args.profile_interval)]


    return cmd


def restart_spark():
    """Stop and start Spark master and worker."""
    spark_home = os.environ["SPARK_HOME"]
    sbin = os.path.join(spark_home, "sbin")
    spark_master = os.environ["SPARK_MASTER"]

    # Stop (ignore errors)
    subprocess.run(
        [os.path.join(sbin, "stop-master.sh")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        [os.path.join(sbin, "stop-worker.sh")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Start (check errors)
    r = subprocess.run([os.path.join(sbin, "start-master.sh")])
    if r.returncode != 0:
        print("Error: Failed to start Spark master", file=sys.stderr)
        sys.exit(1)

    r = subprocess.run([os.path.join(sbin, "start-worker.sh"), spark_master])
    if r.returncode != 0:
        print("Error: Failed to start Spark worker", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Consolidated TPC benchmark runner for Spark-based engines."
    )
    parser.add_argument(
        "--engine",
        required=True,
        help="Engine name (matches a TOML file in engines/)",
    )
    parser.add_argument(
        "--benchmark",
        required=True,
        choices=["tpch", "tpcds"],
        help="Benchmark to run",
    )
    parser.add_argument(
        "--iterations", type=int, default=1, help="Number of iterations (default: 1)"
    )
    parser.add_argument(
        "--output", default=".", help="Output directory (default: .)"
    )
    parser.add_argument(
        "--query", type=int, default=None, help="Run a single query number"
    )
    parser.add_argument(
        "--no-restart",
        action="store_true",
        help="Skip Spark master/worker restart",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the spark-submit command without executing",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable executor metrics profiling via Spark REST API",
    )
    parser.add_argument(
        "--profile-interval",
        type=float,
        default=2.0,
        help="Profiling poll interval in seconds (default: 2.0)",
    )
    args = parser.parse_args()

    config = load_engine_config(args.engine)

    # Apply env defaults and exports before validation
    apply_env_defaults(config)
    apply_env_exports(config)

    check_common_env()
    check_required_env(config)
    check_benchmark_env(config, args.benchmark)

    # Restart Spark unless --no-restart or --dry-run
    if not args.no_restart and not args.dry_run:
        restart_spark()

    cmd = build_spark_submit_cmd(config, args.benchmark, args)

    if args.dry_run:
        # Group paired arguments (e.g. --conf key=value) on one line
        parts = []
        i = 0
        while i < len(cmd):
            token = cmd[i]
            if token.startswith("--") and i + 1 < len(cmd) and not cmd[i + 1].startswith("--"):
                parts.append(f"{token} {cmd[i + 1]}")
                i += 2
            else:
                parts.append(token)
                i += 1
        print(" \\\n    ".join(parts))
    else:
        r = subprocess.run(cmd)
        sys.exit(r.returncode)


if __name__ == "__main__":
    main()

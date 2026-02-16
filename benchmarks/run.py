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
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Unified benchmark runner wrapper.

Reads .conf files, merges them with precedence (profile < engine < CLI),
then builds and executes the spark-submit command.

Usage::

    # = comet-tpch.sh
    python benchmarks/run.py --engine comet --profile standalone-tpch \\
        --restart-cluster \\
        -- tpc --benchmark tpch --data $TPCH_DATA --queries $TPCH_QUERIES \\
           --output . --iterations 1



    # = comet-tpch-iceberg.sh (dynamic catalog via --conf)
    python benchmarks/run.py --engine comet-iceberg --profile standalone-tpch \\
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \\
        --conf spark.sql.catalog.local.type=hadoop \\
        --conf spark.sql.catalog.local.warehouse=$ICEBERG_WAREHOUSE \\
        --conf spark.sql.defaultCatalog=local \\
        --restart-cluster \\
        -- tpc --benchmark tpch --catalog local --database tpch \\
           --queries $TPCH_QUERIES --output . --iterations 1

    # shuffle benchmark
    python benchmarks/run.py --engine comet-jvm-shuffle --profile local \\
        -- shuffle --benchmark shuffle-hash --data /tmp/data --mode jvm \\
           --output . --iterations 3
"""

import argparse
import os
import subprocess
import sys

# Allow importing from the repo root so ``from benchmarks.runner.config ...``
# works when this script is run directly.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from benchmarks.runner.config import merge_configs, split_config


def _parse_args():
    """Parse wrapper-level arguments, splitting on ``--``."""
    parser = argparse.ArgumentParser(
        description="Unified benchmark runner — builds and executes spark-submit",
        usage=(
            "%(prog)s --engine NAME [--profile NAME] "
            "[--conf key=value ...] [--restart-cluster] "
            "[--dry-run] -- SUITE_ARGS..."
        ),
    )
    parser.add_argument("--engine", required=True, help="Engine config name")
    parser.add_argument("--profile", default=None, help="Profile config name")
    parser.add_argument(
        "--conf", action="append", default=[],
        help="Extra key=value config override (repeatable)",
    )
    parser.add_argument(
        "--restart-cluster", action="store_true",
        help="Stop and restart Spark standalone master + worker",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the spark-submit command without executing it",
    )

    # Split on "--": everything before goes to this parser, everything after
    # is passed through to the benchmark suite CLI.
    argv = sys.argv[1:]
    if "--" in argv:
        sep = argv.index("--")
        wrapper_args = argv[:sep]
        suite_args = argv[sep + 1:]
    else:
        wrapper_args = argv
        suite_args = []

    args = parser.parse_args(wrapper_args)
    args.suite_args = suite_args
    return args


def _resolve_conf_path(conf_dir, kind, name):
    """Return the path to a .conf file, or exit with an error."""
    path = os.path.join(conf_dir, kind, f"{name}.conf")
    if not os.path.isfile(path):
        available = sorted(
            f.removesuffix(".conf")
            for f in os.listdir(os.path.join(conf_dir, kind))
            if f.endswith(".conf")
        )
        print(
            f"Error: {kind} config '{name}' not found at {path}\n"
            f"Available: {', '.join(available)}",
            file=sys.stderr,
        )
        sys.exit(1)
    return path


def _restart_cluster():
    """Stop and start Spark standalone master + worker."""
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        print("Error: SPARK_HOME must be set for --restart-cluster", file=sys.stderr)
        sys.exit(1)
    spark_master = os.environ.get("SPARK_MASTER")
    if not spark_master:
        print("Error: SPARK_MASTER must be set for --restart-cluster", file=sys.stderr)
        sys.exit(1)

    sbin = os.path.join(spark_home, "sbin")
    print("Restarting Spark standalone cluster...")
    subprocess.run([os.path.join(sbin, "stop-master.sh")], stderr=subprocess.DEVNULL, check=False)
    subprocess.run([os.path.join(sbin, "stop-worker.sh")], stderr=subprocess.DEVNULL, check=False)
    subprocess.check_call([os.path.join(sbin, "start-master.sh")])
    subprocess.check_call([os.path.join(sbin, "start-worker.sh"), spark_master])


def main():
    args = _parse_args()
    conf_dir = os.path.join(_SCRIPT_DIR, "conf")

    # Resolve config file paths
    engine_path = _resolve_conf_path(conf_dir, "engines", args.engine)
    profile_path = (
        _resolve_conf_path(conf_dir, "profiles", args.profile)
        if args.profile else None
    )

    # Merge configs: profile < engine < CLI overrides
    merged = merge_configs(
        profile_path=profile_path,
        engine_path=engine_path,
        cli_overrides=args.conf,
    )
    spark_conf, runner_conf = split_config(merged)

    # Export runner.env.* as environment variables
    for key, value in runner_conf.items():
        if key.startswith("env."):
            env_var = key[len("env."):]
            os.environ[env_var] = value
            print(f"Exported {env_var}={value}")

    # Restart cluster if requested
    if args.restart_cluster:
        _restart_cluster()

    # Build spark-submit command
    spark_home = os.environ.get("SPARK_HOME", "")
    if not spark_home:
        print("Error: SPARK_HOME must be set", file=sys.stderr)
        sys.exit(1)

    cmd = [os.path.join(spark_home, "bin", "spark-submit")]

    # Master
    master = runner_conf.get("master")
    if master:
        cmd += ["--master", master]

    # JARs
    jars = runner_conf.get("jars")
    if jars:
        cmd += ["--jars", jars]
        cmd += ["--driver-class-path", jars.replace(",", ":")]

    # Spark configs
    for key, value in spark_conf.items():
        cmd += ["--conf", f"{key}={value}"]

    # Python script (the CLI entry point)
    cmd.append(os.path.join(_SCRIPT_DIR, "runner", "cli.py"))

    # Inject --name from runner.name if not already in suite args
    runner_name = runner_conf.get("name", args.engine)
    suite_args = list(args.suite_args)
    if "--name" not in suite_args:
        suite_args = ["--name", runner_name] + suite_args

    cmd += suite_args

    # Print and execute
    print()
    print("spark-submit command:")
    print(f"  {' '.join(cmd)}")
    print()

    if args.dry_run:
        return

    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    main()

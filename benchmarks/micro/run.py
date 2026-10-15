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

"""Runner for the Comet micro benchmark suites on a single machine (typically EC2).

The script is self-contained and can be downloaded on its own, before the
repository is cloned:

    curl -sSLO https://raw.githubusercontent.com/apache/datafusion-comet/main/benchmarks/micro/run.py
    python3 run.py all

Subcommands:

    setup     install prerequisites, clone/update the repository, build a release
    run       run the benchmark suites, one JVM per suite
    collect   copy the generated results into benchmarks/results/micro
    publish   commit the collected results and optionally open a pull request
    all       setup + run + collect

Examples:

    python3 run.py all --ref main
    python3 run.py run --only Cast
    python3 run.py run --suites my-suites.txt --heap 12g
    python3 run.py collect --publish
    python3 run.py publish --push --open-pr
"""

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

BENCH_PACKAGE = "org.apache.spark.sql.benchmark"
BENCH_SOURCE_DIR = Path("spark/src/test/scala/org/apache/spark/sql/benchmark")
DEFAULT_REPO = "https://github.com/apache/datafusion-comet.git"
DEFAULT_COMET_HOME = Path.home() / "datafusion-comet"
DEFAULT_RUNS_ROOT = Path.home() / "comet-bench-runs"
DEFAULT_HEAP = "8g"
DEFAULT_JDK = "17"
# Spark 4.x, the default build profile, does not compile on anything older
MINIMUM_JDK = 17
DEFAULT_TIMEOUT_MINUTES = 60
# Only used when the distribution has no protoc package. prost-build needs a
# protoc on the PATH to compile the Comet protobuf definitions.
PROTOC_VERSION = "25.5"
RESULTS_SUBDIR = Path("benchmarks") / "results" / "micro"
IMDS_BASE = "http://169.254.169.254/latest"

# Suites that legitimately take much longer than the default timeout allows.
# Minutes, applied in place of --timeout for that suite only.
SUITE_TIMEOUT_OVERRIDES = {
    "CometShuffleBenchmark": 180,
}

# Suites deliberately left out of the default set, with the reason. They can
# still be run by naming them in a --suites file. Everything else found in
# BENCH_SOURCE_DIR is run, so a new benchmark needs no change here.
EXCLUDED_SUITES = {
    "CometTPCHQueryBenchmark": "needs TPC-H data, pass --data-location",
    "CometTPCDSQueryBenchmark": "needs TPC-DS data, pass --data-location",
    "CometTPCDSMicroBenchmark": "needs TPC-DS data, pass --data-location",
    "CometC2RIsolatedBench": "prints to stdout only, writes no results file",
    "CometReadHdfsBenchmark": "starts a local HDFS mini cluster",
}


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def log(message):
    stamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def fail(message):
    print(f"error: {message}", file=sys.stderr)
    sys.exit(1)


def utc_now():
    return datetime.now(timezone.utc)


def format_duration(seconds):
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{seconds:02d}s"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def run_command(cmd, cwd=None, env=None, log_path=None, timeout=None, check=True, dry_run=False):
    """Run a command, optionally sending its output to a log file."""
    printable = " ".join(str(part) for part in cmd)
    if dry_run:
        log(f"[dry-run] {printable}")
        return 0
    log(printable)
    if log_path is None:
        completed = subprocess.run(cmd, cwd=cwd, env=env, timeout=timeout)
    else:
        with open(log_path, "w") as handle:
            handle.write(f"$ {printable}\n\n")
            handle.flush()
            completed = subprocess.run(
                cmd, cwd=cwd, env=env, stdout=handle, stderr=subprocess.STDOUT, timeout=timeout
            )
    if check and completed.returncode != 0:
        fail(f"command failed with exit code {completed.returncode}: {printable}")
    return completed.returncode


def capture(cmd, cwd=None, env=None):
    """Run a command and return stripped stdout, or None if it fails."""
    try:
        completed = subprocess.run(
            cmd, cwd=cwd, env=env, capture_output=True, text=True, timeout=300
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def sudo_prefix():
    if os.geteuid() == 0:
        return []
    if shutil.which("sudo") is None:
        fail("this step needs root privileges but sudo was not found")
    return ["sudo"]


def tail_file(path, lines=20):
    try:
        content = Path(path).read_text(errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(content[-lines:])


def read_proc_field(path, prefix):
    """The value after the colon of the first line in `path` starting with `prefix`."""
    try:
        for line in Path(path).read_text().splitlines():
            if line.startswith(prefix):
                return line.split(":", 1)[1].strip()
    except (OSError, IndexError):
        pass
    return None


# The last lines of a failed run are Maven's epilogue, which says nothing about
# the cause. Look for the exception that started it instead.
EXCEPTION_PATTERN = re.compile(r"^\s*[\w.$]+(Exception|Error)(:|\b)")
MAVEN_FAILURE_PATTERN = re.compile(r"^\[ERROR\] Failed to execute goal")


def failure_excerpt(path, context=12, fallback_lines=20):
    """The first exception in a log, falling back to Maven's message or the tail."""
    try:
        content = Path(path).read_text(errors="replace").splitlines()
    except OSError:
        return ""
    for pattern in (EXCEPTION_PATTERN, MAVEN_FAILURE_PATTERN):
        for index, line in enumerate(content):
            if pattern.search(line):
                return "\n".join(content[index : index + context])
    return "\n".join(content[-fallback_lines:])


# ---------------------------------------------------------------------------
# environment
# ---------------------------------------------------------------------------


def find_checkout_from_script():
    """Return the Comet checkout this script lives in, if any."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "Makefile").is_file() and (parent / "native" / "Cargo.toml").is_file():
            return parent
    return None


def resolve_comet_home(args):
    if args.comet_home:
        return Path(args.comet_home).expanduser().resolve()
    if os.environ.get("COMET_HOME"):
        return Path(os.environ["COMET_HOME"]).expanduser().resolve()
    checkout = find_checkout_from_script()
    if checkout is not None:
        return checkout
    return DEFAULT_COMET_HOME


def require_comet_home(args):
    comet_home = resolve_comet_home(args)
    if not (comet_home / "Makefile").is_file():
        fail(f"{comet_home} is not a Comet checkout, run `setup` first or pass --comet-home")
    return comet_home


def detect_java_home():
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    candidates = []
    for pattern in ("java-*-amazon-corretto*", "java-*-openjdk*", "jdk-*"):
        candidates.extend(Path("/usr/lib/jvm").glob(pattern))

    def version_of(path):
        match = re.search(r"(\d+)", path.name)
        return int(match.group(1)) if match else 0

    # Newest first, so that an older JDK left on the machine is not picked up
    ordered = sorted(candidates, key=version_of, reverse=True)
    for candidate in ordered:
        if (candidate / "bin" / "javac").is_file():
            return str(candidate)
    java = shutil.which("java")
    if java is not None:
        resolved = Path(java).resolve()
        return str(resolved.parent.parent)
    return None


def base_env(comet_home):
    """Environment shared by the build and the benchmark runs."""
    env = dict(os.environ)
    cargo_bin = Path.home() / ".cargo" / "bin"
    if cargo_bin.is_dir() and str(cargo_bin) not in env.get("PATH", ""):
        env["PATH"] = f"{cargo_bin}{os.pathsep}{env.get('PATH', '')}"
    java_home = detect_java_home()
    if java_home:
        env["JAVA_HOME"] = java_home
        java_bin = Path(java_home) / "bin"
        if str(java_bin) not in env.get("PATH", ""):
            env["PATH"] = f"{java_bin}{os.pathsep}{env.get('PATH', '')}"
    env["COMET_CONF_DIR"] = str(comet_home / "conf")
    return env


def java_major_version(env):
    """Major version of the JDK the build will use, or None if unknown."""
    javac = Path(env["JAVA_HOME"]) / "bin" / "javac" if env.get("JAVA_HOME") else "javac"
    try:
        completed = subprocess.run(
            [str(javac), "-version"], capture_output=True, text=True, env=env, timeout=60
        )
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r"(\d+)", (completed.stdout or "") + (completed.stderr or ""))
    return int(match.group(1)) if match else None


def require_supported_jdk(env):
    """Spark 4.x needs JDK 17, and javac silently stubs java.lang.Record below it."""
    major = java_major_version(env)
    if major is None:
        fail(f"could not run javac from JAVA_HOME={env.get('JAVA_HOME')}, install a JDK")
    if major < MINIMUM_JDK:
        fail(
            f"JDK {major} found at JAVA_HOME={env.get('JAVA_HOME')}, but the default Spark 4.x "
            f"profile needs JDK {MINIMUM_JDK} or later. Compiling with an older JDK fails with "
            "'Class java.lang.Record not found'. Install a newer JDK, point JAVA_HOME at it, and "
            "run `./mvnw clean` to discard classes built against the old one."
        )


# ---------------------------------------------------------------------------
# setup
# ---------------------------------------------------------------------------


def install_packages(args):
    dnf_packages = [
        "git",
        "make",
        "cmake",
        "gcc",
        "gcc-c++",
        "protobuf-compiler",
        "python3",
        f"java-{args.jdk}-amazon-corretto-devel",
    ]
    apt_packages = [
        "git",
        "make",
        "cmake",
        "build-essential",
        "protobuf-compiler",
        "python3",
        "curl",
        f"openjdk-{args.jdk}-jdk",
    ]
    if shutil.which("dnf"):
        run_command(sudo_prefix() + ["dnf", "install", "-y"] + dnf_packages, dry_run=args.dry_run)
    elif shutil.which("yum"):
        run_command(sudo_prefix() + ["yum", "install", "-y"] + dnf_packages, dry_run=args.dry_run)
    elif shutil.which("apt-get"):
        run_command(sudo_prefix() + ["apt-get", "update"], dry_run=args.dry_run)
        run_command(
            sudo_prefix() + ["apt-get", "install", "-y"] + apt_packages, dry_run=args.dry_run
        )
    else:
        log("no supported package manager found, skipping package installation")
    install_protoc(args)


def protoc_asset_name():
    machine = platform.machine().lower()
    if platform.system() == "Darwin":
        return f"protoc-{PROTOC_VERSION}-osx-universal_binary.zip"
    if machine in ("aarch64", "arm64"):
        return f"protoc-{PROTOC_VERSION}-linux-aarch_64.zip"
    return f"protoc-{PROTOC_VERSION}-linux-x86_64.zip"


def install_protoc(args):
    """Install protoc from the protobuf releases when the distribution has none.

    Only the Rust build needs a protoc on the PATH. The JVM side downloads its
    own through protoc-jar-maven-plugin.
    """
    if shutil.which("protoc"):
        log(f"protoc already installed: {capture(['protoc', '--version'])}")
        return
    asset = protoc_asset_name()
    url = f"https://github.com/protocolbuffers/protobuf/releases/download/v{PROTOC_VERSION}/{asset}"
    if args.dry_run:
        log(f"[dry-run] download {url} and install bin/protoc into /usr/local/bin")
        return
    log(f"protoc not found, installing {PROTOC_VERSION} from {url}")
    with tempfile.TemporaryDirectory() as work_dir:
        archive = Path(work_dir) / asset
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                archive.write_bytes(response.read())
            with zipfile.ZipFile(archive) as bundle:
                bundle.extract("bin/protoc", work_dir)
        except (OSError, urllib.error.URLError, zipfile.BadZipFile) as error:
            fail(f"could not install protoc: {error}")
        run_command(
            sudo_prefix() + ["install", "-m", "755", f"{work_dir}/bin/protoc", "/usr/local/bin/"]
        )
    log(f"installed {capture(['/usr/local/bin/protoc', '--version'])}")


def install_rust(args):
    if shutil.which("cargo") or (Path.home() / ".cargo" / "bin" / "cargo").is_file():
        log("rust toolchain already installed")
        return
    log("installing rust toolchain")
    if args.dry_run:
        log("[dry-run] curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y")
        return
    installer = subprocess.run(
        ["curl", "--proto", "=https", "--tlsv1.2", "-sSf", "https://sh.rustup.rs"],
        capture_output=True,
    )
    if installer.returncode != 0:
        fail("failed to download rustup")
    shell = subprocess.run(["sh", "-s", "--", "-y"], input=installer.stdout)
    if shell.returncode != 0:
        fail("rustup installation failed")


def clone_or_update(args, comet_home):
    # .git is a directory in a normal clone and a file in a worktree
    if not (comet_home / ".git").exists():
        comet_home.parent.mkdir(parents=True, exist_ok=True)
        run_command(
            ["git", "clone", args.repo, str(comet_home)],
            dry_run=args.dry_run,
        )
        ref = args.ref or "main"
    else:
        log(f"using existing checkout at {comet_home}")
        ref = args.ref
    if ref:
        run_command(["git", "fetch", "origin", "--tags"], cwd=comet_home, dry_run=args.dry_run)
        run_command(["git", "checkout", ref], cwd=comet_home, dry_run=args.dry_run)
        # Fast-forward when the ref is a branch. Detached heads and tags fail
        # here, which is fine.
        run_command(
            ["git", "merge", "--ff-only", f"origin/{ref}"],
            cwd=comet_home,
            check=False,
            dry_run=args.dry_run,
        )


def build_release(args, comet_home):
    env = base_env(comet_home)
    if env.get("JAVA_HOME") is None:
        fail("JAVA_HOME could not be determined, install a JDK or export JAVA_HOME")
    require_supported_jdk(env)
    if args.profile:
        env["PROFILES"] = args.profile
    log("building Comet in release mode, this takes a while")
    run_command(["make", "release"], cwd=comet_home, env=env, dry_run=args.dry_run)


def cmd_setup(args):
    comet_home = resolve_comet_home(args)
    if not args.skip_packages:
        install_packages(args)
        install_rust(args)
    clone_or_update(args, comet_home)
    if not args.skip_build:
        build_release(args, comet_home)
    log(f"setup complete: {comet_home}")
    return comet_home


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


# Top-level `object Name`, with whatever it extends up to the start of its body
OBJECT_PATTERN = re.compile(r"^object\s+(\w+)\s*(extends[^{]*)?\{", re.MULTILINE)


def discover_suites(comet_home):
    """The benchmark entry points, read from the sources rather than listed here.

    An entry point is a top-level `object` in the benchmark package that either
    mixes in a `*Benchmark*` base, which supplies `main`, or declares its own.
    Helpers such as `TPCDSSchemaHelper` do neither and are skipped. Discovering
    them this way means a newly added benchmark runs without anyone having to
    remember to also add it to this file.
    """
    source_dir = comet_home / BENCH_SOURCE_DIR
    if not source_dir.is_dir():
        fail(f"no benchmark sources at {source_dir}, is {comet_home} a Comet checkout?")
    suites = set()
    for path in sorted(source_dir.glob("*.scala")):
        text = path.read_text(errors="replace")
        declares_main = "def main(" in text
        for name, bases in OBJECT_PATTERN.findall(text):
            if "Benchmark" in bases or declares_main:
                suites.add(name)
    if not suites:
        fail(f"no benchmark objects found in {source_dir}")
    return sorted(suites)


def stale_suite_entries(comet_home):
    """Names in this file that no longer match the benchmark sources.

    EXCLUDED_SUITES and SUITE_TIMEOUT_OVERRIDES are keyed by suite name, so a
    rename upstream silently turns an exclusion into a suite that runs, or a
    timeout override into one that does not apply. Report them instead.
    """
    discovered = set(discover_suites(comet_home))
    return [
        f"{suite} is listed in {label} but is not a benchmark in {BENCH_SOURCE_DIR}"
        for label, names in (
            ("EXCLUDED_SUITES", EXCLUDED_SUITES),
            ("SUITE_TIMEOUT_OVERRIDES", SUITE_TIMEOUT_OVERRIDES),
        )
        for suite in sorted(set(names) - discovered)
    ]


def load_suites(args, comet_home):
    if args.suites:
        path = Path(args.suites).expanduser()
        if not path.is_file():
            fail(f"suite file not found: {path}")
        suites = []
        for line in path.read_text().splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                suites.append(line.rsplit(".", 1)[-1])
    else:
        suites = [s for s in discover_suites(comet_home) if s not in EXCLUDED_SUITES]
    if args.only:
        suites = [s for s in suites if any(re.search(pattern, s) for pattern in args.only)]
    if args.skip:
        suites = [s for s in suites if not any(re.search(pattern, s) for pattern in args.skip)]
    if not suites:
        fail("no suites selected")
    return suites


def benchmark_args(comet_home, env, profile, heap):
    """The benchmark invocation, read from the Makefile's `print-benchmark-args`.

    The Makefile is the one definition of how a suite is invoked. The
    `benchmark-%` target itself cannot be used here because its `release`
    prerequisite would rebuild Comet before every suite.
    """
    command = ["make", "print-benchmark-args", f"BENCH_HEAP={heap}"]
    if profile:
        command.append(f"PROFILES={profile}")
    output = capture(command, cwd=comet_home, env=env)
    if output is None:
        fail(f"`{' '.join(command)}` failed in {comet_home}, is make installed?")
    values = {}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    missing = [key for key in ("MAVEN_OPTS", "MVN_ARGS") if not values.get(key)]
    if missing:
        fail(f"print-benchmark-args did not report {', '.join(missing)}")
    return values


def suite_command(suite, mvn_args, profile):
    """The `mvnw` invocation for one suite, run from the `spark` directory."""
    command = ["../mvnw"] + mvn_args.split() + [f"-Dexec.mainClass={BENCH_PACKAGE}.{suite}"]
    if profile:
        command.append(profile)
    return command


def warn_unless_release_build(comet_home):
    release_dir = comet_home / "native" / "target" / "release"
    if any((release_dir / name).is_file() for name in ("libcomet.so", "libcomet.dylib")):
        return
    log(
        "warning: no native release build found in native/target/release. "
        "Run `setup` or `make release` first, otherwise the numbers will not reflect "
        "a release build."
    )


def git_info(comet_home):
    return {
        "commit": capture(["git", "rev-parse", "HEAD"], cwd=comet_home),
        "commit_short": capture(["git", "rev-parse", "--short", "HEAD"], cwd=comet_home),
        "branch": capture(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=comet_home),
        "describe": capture(["git", "log", "-1", "--pretty=%s"], cwd=comet_home),
    }


def cmd_run(args, comet_home=None):
    comet_home = comet_home or require_comet_home(args)
    suites = load_suites(args, comet_home)
    for problem in stale_suite_entries(comet_home):
        log(f"warning: {problem}")
    if args.list:
        for suite in suites:
            print(f"{BENCH_PACKAGE}.{suite}")
        for suite, reason in sorted(EXCLUDED_SUITES.items()):
            print(f"# {BENCH_PACKAGE}.{suite} ({reason})")
        return None

    warn_unless_release_build(comet_home)
    runs_root = Path(args.runs_root).expanduser()
    run_dir = runs_root / utc_now().strftime("%Y%m%dT%H%M%SZ")
    log_dir = run_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    env = base_env(comet_home)
    if env.get("JAVA_HOME") is None:
        fail("JAVA_HOME could not be determined, install a JDK or export JAVA_HOME")
    require_supported_jdk(env)
    bench = benchmark_args(comet_home, env, args.profile, args.heap)
    env["MAVEN_OPTS"] = bench["MAVEN_OPTS"]
    env["SPARK_GENERATE_BENCHMARK_FILES"] = "1"

    spark_dir = comet_home / "spark"
    started = time.time()
    started_utc = utc_now()
    log(f"running {len(suites)} suite(s), logs in {log_dir}")
    results = []

    for index, suite in enumerate(suites, start=1):
        log_path = log_dir / f"{suite}.log"
        timeout_minutes = SUITE_TIMEOUT_OVERRIDES.get(suite, args.timeout)
        timeout = timeout_minutes * 60 if timeout_minutes else None
        if args.timeout and timeout_minutes != args.timeout:
            log(f"[{index}/{len(suites)}] {suite} (timeout raised to {timeout_minutes}m)")
        else:
            log(f"[{index}/{len(suites)}] {suite}")
        suite_started = time.time()
        status = "ok"
        try:
            code = run_command(
                suite_command(suite, bench["MVN_ARGS"], args.profile),
                cwd=spark_dir,
                env=env,
                log_path=log_path,
                timeout=timeout,
                check=False,
                dry_run=args.dry_run,
            )
            if code != 0:
                status = "failed"
        except subprocess.TimeoutExpired:
            status = "timeout"
        duration = time.time() - suite_started
        results.append(
            {
                "suite": suite,
                "status": status,
                "duration_seconds": round(duration, 1),
                "log": str(log_path),
            }
        )
        if status == "ok":
            log(f"    ok in {format_duration(duration)}")
        else:
            excerpt = tail_file(log_path) if status == "timeout" else failure_excerpt(log_path)
            log(f"    {status} after {format_duration(duration)}, from {log_path}:")
            print(excerpt, flush=True)

    summary = {
        "started_utc": started_utc.isoformat(),
        "finished_utc": utc_now().isoformat(),
        "duration_seconds": round(time.time() - started, 1),
        "comet_home": str(comet_home),
        "profile": args.profile or "default",
        "heap": args.heap,
        "git": git_info(comet_home),
        "suites": results,
    }
    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")

    failed = [entry for entry in results if entry["status"] != "ok"]
    log(f"finished in {format_duration(summary['duration_seconds'])}, summary: {summary_path}")
    for entry in results:
        marker = " " if entry["status"] == "ok" else "!"
        log(
            f"  {marker} {entry['suite']:<45} {entry['status']:<8} "
            f"{format_duration(entry['duration_seconds'])}"
        )
    if failed:
        log(f"{len(failed)} suite(s) did not complete successfully")
    return run_dir


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------


def imds(path):
    """Read an EC2 instance metadata value, returning None when not on EC2."""
    try:
        token_request = urllib.request.Request(
            f"{IMDS_BASE}/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        token = urllib.request.urlopen(token_request, timeout=2).read().decode()
        value_request = urllib.request.Request(
            f"{IMDS_BASE}/meta-data/{path}",
            headers={"X-aws-ec2-metadata-token": token},
        )
        return urllib.request.urlopen(value_request, timeout=2).read().decode()
    except Exception:
        return None


def machine_info(env):
    info = {
        "instance_type": imds("instance-type") or "unknown (not an EC2 instance?)",
        "availability_zone": imds("placement/availability-zone") or "unknown",
        "cpus": str(os.cpu_count()),
        "cpu_model": "unknown",
        "memory": "unknown",
        "os": platform.platform(),
        "java": "unknown",
        "rustc": capture(["rustc", "--version"], env=env) or "unknown",
    }
    cpu_model = read_proc_field("/proc/cpuinfo", "model name")
    if cpu_model:
        info["cpu_model"] = cpu_model
    mem_total = read_proc_field("/proc/meminfo", "MemTotal")
    if mem_total:
        # "16311236 kB"
        info["memory"] = f"{int(mem_total.split()[0]) / 1024 / 1024:.1f} GiB"
    if info["cpu_model"] == "unknown" and platform.system() == "Darwin":
        # /proc does not exist on macOS, which is where a local collect runs
        info["cpu_model"] = capture(["sysctl", "-n", "machdep.cpu.brand_string"]) or "unknown"
        memory_bytes = capture(["sysctl", "-n", "hw.memsize"])
        if memory_bytes:
            info["memory"] = f"{int(memory_bytes) / 1024**3:.1f} GiB"
    # `java -version` writes to stderr rather than stdout
    java_version = None
    try:
        completed = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, env=env, timeout=60
        )
        java_version = (completed.stderr or completed.stdout).strip().splitlines()[0]
    except (OSError, subprocess.SubprocessError, IndexError):
        pass
    info["java"] = java_version or "unknown"
    return info


def latest_run_dir(runs_root):
    runs_root = Path(runs_root).expanduser()
    if not runs_root.is_dir():
        return None
    candidates = sorted(path for path in runs_root.iterdir() if (path / "summary.json").is_file())
    return candidates[-1] if candidates else None


LICENSE_HEADER = """<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
"""


def write_run_info(destination, summary, info, copied):
    git = summary.get("git", {})
    lines = [
        LICENSE_HEADER,
        "# Comet Micro Benchmark Run",
        "",
        "Generated by `benchmarks/micro/run.py collect`.",
        "",
        "## Environment",
        "",
        "| Field | Value |",
        "| --- | --- |",
        f"| Date (UTC) | {summary.get('started_utc', 'unknown')} |",
        f"| Instance type | {info['instance_type']} |",
        f"| Availability zone | {info['availability_zone']} |",
        f"| CPU | {info['cpu_model']} ({info['cpus']} vCPU) |",
        f"| Memory | {info['memory']} |",
        f"| OS | {info['os']} |",
        f"| Java | {info['java']} |",
        f"| Rust | {info['rustc']} |",
        f"| Maven profile | {summary.get('profile', 'default')} |",
        f"| Benchmark heap | {summary.get('heap', 'unknown')} |",
        f"| Comet branch | {git.get('branch', 'unknown')} |",
        f"| Comet commit | {git.get('commit', 'unknown')} |",
        f"| Total run time | {format_duration(summary.get('duration_seconds', 0))} |",
        "",
        "## Suites",
        "",
        "| Suite | Status | Duration | Results file |",
        "| --- | --- | --- | --- |",
    ]
    for entry in summary.get("suites", []):
        results_file = copied.get(entry["suite"], "-")
        lines.append(
            f"| {entry['suite']} | {entry['status']} | "
            f"{format_duration(entry['duration_seconds'])} | {results_file} |"
        )
    lines.append("")
    (destination / "RUN-INFO.md").write_text("\n".join(lines))


def cmd_collect(args, comet_home=None, run_dir=None):
    comet_home = comet_home or require_comet_home(args)
    if run_dir is None:
        run_dir = (
            Path(args.run_dir).expanduser() if args.run_dir else latest_run_dir(args.runs_root)
        )
    if run_dir is None or not (run_dir / "summary.json").is_file():
        fail("no benchmark run found, pass --run-dir or run `run` first")
    summary = json.loads((run_dir / "summary.json").read_text())

    source = comet_home / "spark" / "benchmarks"
    if not source.is_dir():
        fail(f"no results directory at {source}, did the suites run?")
    destination = comet_home / RESULTS_SUBDIR
    destination.mkdir(parents=True, exist_ok=True)

    started = datetime.fromisoformat(summary["started_utc"]).timestamp()
    copied = {}
    for path in sorted(source.glob("*.txt")):
        if not args.all_results and path.stat().st_mtime < started:
            continue
        shutil.copy2(path, destination / path.name)
        suite = path.name.split("-")[0]
        copied[suite] = path.name
        log(f"collected {path.name}")
    if not copied:
        log("no results files were produced by this run")

    info = machine_info(base_env(comet_home))
    write_run_info(destination, summary, info, copied)
    log(f"wrote {destination / 'RUN-INFO.md'}")
    log(f"results are in {destination}")
    if args.publish:
        cmd_publish(args, comet_home=comet_home)
    return destination


# ---------------------------------------------------------------------------
# publish
# ---------------------------------------------------------------------------


PR_BODY_TEMPLATE = """## Which issue does this PR close?

N/A

## Rationale for this change

Refresh the checked-in micro benchmark results so that changes in expression and
operator performance are visible over time.

## What changes are included in this PR?

Micro benchmark results produced by `benchmarks/micro/run.py` on a {instance_type}
instance, from Comet commit {commit}.

## How are these changes tested?

These are benchmark result files only, produced by running the micro benchmark
suites. See `benchmarks/results/micro/RUN-INFO.md` for the environment details.
"""


def cmd_publish(args, comet_home=None):
    comet_home = comet_home or require_comet_home(args)
    destination = comet_home / RESULTS_SUBDIR
    if not destination.is_dir():
        fail(f"no collected results at {destination}, run `collect` first")

    run_info = destination / "RUN-INFO.md"
    instance_type = "unknown"
    if run_info.is_file():
        for line in run_info.read_text().splitlines():
            if line.startswith("| Instance type |"):
                instance_type = line.split("|")[2].strip()
                break
    commit = git_info(comet_home).get("commit_short") or "unknown"
    branch = args.branch or f"benchmark-results-{utc_now().strftime('%Y%m%d')}"
    title = "chore: add micro benchmark results"
    if not instance_type.startswith("unknown"):
        title = f"{title} from {instance_type}"

    run_command(["git", "checkout", "-b", branch], cwd=comet_home, dry_run=args.dry_run)
    run_command(
        ["git", "add", str(RESULTS_SUBDIR)],
        cwd=comet_home,
        dry_run=args.dry_run,
    )
    run_command(
        ["git", "commit", "-m", title],
        cwd=comet_home,
        dry_run=args.dry_run,
    )

    if not args.push:
        log("commit created, push it and open a pull request with:")
        print(f"  git -C {comet_home} push -u {args.remote} {branch}")
        print(f'  gh pr create --base {args.base} --title "{title}"')
        return

    run_command(
        ["git", "push", "-u", args.remote, branch],
        cwd=comet_home,
        dry_run=args.dry_run,
    )
    if not args.open_pr:
        log("branch pushed, open the pull request when ready")
        return
    if shutil.which("gh") is None:
        log("gh is not installed, open the pull request manually")
        return
    body_path = comet_home / ".git" / "comet-benchmark-pr-body.md"
    body_path.write_text(PR_BODY_TEMPLATE.format(instance_type=instance_type, commit=commit))
    run_command(
        [
            "gh",
            "pr",
            "create",
            "--base",
            args.base,
            "--title",
            title,
            "--body-file",
            str(body_path),
        ],
        cwd=comet_home,
        dry_run=args.dry_run,
    )


# ---------------------------------------------------------------------------
# all
# ---------------------------------------------------------------------------


def cmd_all(args):
    comet_home = cmd_setup(args)
    run_dir = cmd_run(args, comet_home=comet_home)
    if run_dir is None:
        return
    cmd_collect(args, comet_home=comet_home, run_dir=run_dir)


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------


def add_common_arguments(parser):
    parser.add_argument(
        "--comet-home",
        help="Comet checkout to use (default: the checkout this script lives in, "
        f"otherwise {DEFAULT_COMET_HOME})",
    )
    parser.add_argument("--profile", help="Maven profile to build and run with, e.g. -Pspark-3.5")
    parser.add_argument(
        "--dry-run", action="store_true", help="print the commands without running them"
    )


def add_setup_arguments(parser):
    parser.add_argument("--repo", default=DEFAULT_REPO, help="repository to clone")
    parser.add_argument("--ref", help="branch, tag or commit to check out")
    parser.add_argument("--jdk", default=DEFAULT_JDK, help="JDK major version to install")
    parser.add_argument(
        "--skip-packages", action="store_true", help="do not install system packages or Rust"
    )
    parser.add_argument("--skip-build", action="store_true", help="do not run `make release`")


def add_run_arguments(parser):
    parser.add_argument("--suites", help="file listing suites to run, one per line")
    parser.add_argument(
        "--only", action="append", metavar="REGEX", help="only run suites matching this pattern"
    )
    parser.add_argument(
        "--skip", action="append", metavar="REGEX", help="skip suites matching this pattern"
    )
    parser.add_argument("--heap", default=DEFAULT_HEAP, help="JVM max heap for each suite")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_MINUTES,
        help="per-suite timeout in minutes, 0 to disable",
    )
    parser.add_argument("--list", action="store_true", help="list the selected suites and exit")


def add_runs_root_argument(parser):
    parser.add_argument(
        "--runs-root", default=str(DEFAULT_RUNS_ROOT), help="directory holding per-run logs"
    )


def add_collect_arguments(parser):
    parser.add_argument("--run-dir", help="run directory to collect (default: most recent)")
    parser.add_argument(
        "--all-results",
        action="store_true",
        help="collect every results file, not only those written by this run",
    )
    parser.add_argument(
        "--publish", action="store_true", help="commit the results after collecting them"
    )


def add_publish_arguments(parser):
    parser.add_argument("--branch", help="branch name to create (default: benchmark-results-DATE)")
    parser.add_argument("--remote", default="origin", help="git remote to push to")
    parser.add_argument("--base", default="main", help="base branch for the pull request")
    parser.add_argument("--push", action="store_true", help="push the branch to the remote")
    parser.add_argument(
        "--open-pr", action="store_true", help="open a pull request with gh after pushing"
    )


def parse_arguments(argv):
    parser = argparse.ArgumentParser(
        description="Run the Comet micro benchmark suites and publish the results.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    setup = subparsers.add_parser("setup", help="install prerequisites and build Comet")
    add_common_arguments(setup)
    add_setup_arguments(setup)
    setup.set_defaults(func=cmd_setup)

    run = subparsers.add_parser("run", help="run the benchmark suites")
    add_common_arguments(run)
    add_run_arguments(run)
    add_runs_root_argument(run)
    run.set_defaults(func=cmd_run)

    collect = subparsers.add_parser("collect", help="copy results into benchmarks/results/micro")
    add_common_arguments(collect)
    add_collect_arguments(collect)
    add_runs_root_argument(collect)
    add_publish_arguments(collect)
    collect.set_defaults(func=cmd_collect)

    publish = subparsers.add_parser("publish", help="commit and optionally open a pull request")
    add_common_arguments(publish)
    add_publish_arguments(publish)
    publish.set_defaults(func=cmd_publish)

    every = subparsers.add_parser("all", help="setup, run and collect in one go")
    add_common_arguments(every)
    add_setup_arguments(every)
    add_run_arguments(every)
    add_collect_arguments(every)
    add_runs_root_argument(every)
    add_publish_arguments(every)
    every.set_defaults(func=cmd_all)

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_arguments(argv if argv is not None else sys.argv[1:])
    if getattr(args, "timeout", None) == 0:
        args.timeout = None
    args.func(args)


if __name__ == "__main__":
    main()

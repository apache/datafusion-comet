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

"""Capture one pair of diagnostics when a Spark test console stops changing.

Run beside the existing test command, with --root-pid set to its owning shell.
Only that shell's descendants are eligible for jcmd; the watchdog's own subtree
is excluded. Identity and ancestry are checked again immediately before attach.
The tool never retries tests, kills tests, or supplies a test verdict. Its exit
code describes diagnostics only (1 means incomplete); the caller must preserve
the original test command's status and stop this companion in an EXIT trap.

After 300 seconds of log inactivity, capture two snapshots, waiting 10 seconds
after the first completes. Each includes a process tree, available OS/cgroup
memory counters, and `jcmd PID Thread.print -l` for at most eight descendant JVMs.
Each command has a timeout (15 seconds for jcmd) and output limit (2 MiB per
dump). There is only one pair, bounding default collection to roughly six
minutes and 40 MiB. The companion then waits quietly for shutdown, so its PID
remains safe for the caller's cleanup trap. Inactivity is not a test failure.

SIGTERM/SIGINT or root exit stops collection promptly. SIGTERM, then SIGKILL if
needed, is sent ONLY to diagnostic subprocess groups created by this tool.
No explicit signal is sent to the watched tree. jcmd uses the JDK's normal
attach mechanism, which may signal the target JVM to start its attach listener.
Linux provides /proc and cgroup v2 counters; macOS supplies a process tree and
vm_stat instead. Missing tools/counters and failed attaches are recorded.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import time
from typing import NamedTuple


MAX_JVMS = 8
MAX_DUMP_BYTES = 2 * 1024 * 1024
MAX_PS_BYTES = 1024 * 1024
MAX_COUNTER_BYTES = 64 * 1024


class Process(NamedTuple):
    pid: int
    ppid: int
    state: str
    identity: str
    command: str


class CommandResult(NamedTuple):
    output: bytes
    returncode: object
    error: str


def process_identity(pid):
    """Return a live process's start identity, rejecting zombies and reused PIDs."""
    if sys.platform.startswith("linux"):
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            return None if fields[0] == "Z" else "proc:" + fields[19]
        except (OSError, IndexError):
            return None
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat=,lstart="],
            capture_output=True, text=True, timeout=0.5, check=False)
        fields = result.stdout.split()
        if result.returncode == 0 and len(fields) == 6 and not fields[0].startswith("Z"):
            return "ps:" + " ".join(fields[1:])
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


class Stop:
    def __init__(self, root_pid):
        self.root_pid = root_pid
        self.identity = process_identity(root_pid)
        self.requested = False
        self.last_check = 0.0

    def handle_signal(self, _signum, _frame):
        self.requested = True

    def __call__(self):
        if not self.requested and time.monotonic() - self.last_check >= 0.2:
            self.last_check = time.monotonic()
            self.requested = self.identity is None or (
                process_identity(self.root_pid) != self.identity)
        return self.requested

    def wait(self, seconds):
        deadline = time.monotonic() + seconds
        while not self():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            time.sleep(min(0.1, remaining))
        return False


def stop_command(command):
    """Clean up only the process group started by run_command."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(command.pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            # macOS can return EPERM, rather than ESRCH, for an empty group
            # after TERM. A still-running diagnostic must not be ignored.
            if command.poll() is None:
                raise
        if sig == signal.SIGTERM:
            time.sleep(0.1)
    try:
        command.wait(timeout=0.5)
    except subprocess.TimeoutExpired:
        pass


def run_command(args, timeout, limit, stopped):
    """Read a bounded pipe; never let a stuck attach delay cancellation."""
    output = bytearray()
    if stopped():
        return CommandResult(b"", None, "cancelled before diagnostic command started")
    try:
        command = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            start_new_session=True)
    except OSError as error:
        return CommandResult(b"", None, f"cannot start {args[0]}: {error}")
    error = ""
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(command.stdout, selectors.EVENT_READ)
            while True:
                if stopped():
                    error = "cancelled: root exited or watchdog received a stop signal"
                    break
                if time.monotonic() >= deadline:
                    error = f"command timed out after {timeout:g} seconds"
                    break
                ready = selector.select(timeout=max(0, min(0.1, deadline - time.monotonic())))
                if ready:
                    chunk = os.read(command.stdout.fileno(), 65536)
                    if not chunk:
                        if command.poll() is not None:
                            break
                        continue
                    room = limit - len(output)
                    output.extend(chunk[:room])
                    if len(chunk) > room:
                        error = f"output exceeded {limit} bytes; diagnostic command stopped"
                        break
            if error:
                stop_command(command)
            else:
                command.wait(timeout=0.5)
    finally:
        if command.poll() is None:
            stop_command(command)
        command.stdout.close()
    if not error and command.returncode:
        error = f"command exited with status {command.returncode}"
    return CommandResult(bytes(output), command.returncode, error)


def read_processes(stopped):
    result = run_command(
        ["ps", "-axo", "pid=,ppid=,stat=,lstart=,comm="], 5, MAX_PS_BYTES, stopped)
    if result.error:
        raise RuntimeError("process listing failed: " + result.error)
    processes = {}
    for line in result.output.decode(errors="replace").splitlines():
        fields = line.split(None, 8)
        if len(fields) != 9:
            continue
        pid, ppid = int(fields[0]), int(fields[1])
        identity = (process_identity(pid) if sys.platform.startswith("linux")
                    else "ps:" + " ".join(fields[3:8]))
        if identity and not fields[2].startswith("Z"):
            processes[pid] = Process(pid, ppid, fields[2], identity, fields[8])
    return processes


def descendants(processes, root_pid, excluded_pid):
    children = defaultdict(list)
    for process in processes.values():
        children[process.ppid].append(process.pid)
    selected = {}
    pending = [root_pid]
    visited = {root_pid}
    while pending:
        for pid in children[pending.pop()]:
            if pid == excluded_pid or pid in visited:
                continue
            visited.add(pid)
            selected[pid] = processes[pid]
            pending.append(pid)
    return selected


def read_counter(path):
    try:
        with path.open("rb") as source:
            data = source.read(MAX_COUNTER_BYTES + 1)
        if len(data) > MAX_COUNTER_BYTES:
            return data[:MAX_COUNTER_BYTES] + b"\n[truncated]\n"
        return data
    except OSError as error:
        return f"unavailable: {error}\n".encode()


def memory_snapshot(pids, stopped):
    if not sys.platform.startswith("linux"):
        result = run_command(["vm_stat"], 5, MAX_COUNTER_BYTES, stopped)
        return result.output + ("\n" + result.error + "\n").encode()
    output = bytearray(b"=== /proc/meminfo ===\n")
    output.extend(read_counter(Path("/proc/meminfo")))
    seen = set()
    for pid in pids:
        output.extend(f"\n=== PID {pid} cgroups ===\n".encode())
        membership = read_counter(Path(f"/proc/{pid}/cgroup"))
        output.extend(membership)
        for line in membership.decode(errors="replace").splitlines():
            if not line.startswith("0::"):
                continue
            relative = Path(line[3:].lstrip("/"))
            if ".." in relative.parts:
                output.extend(b"cgroup v2 path unavailable in this mount namespace\n")
                continue
            group = Path("/sys/fs/cgroup") / relative
            if group in seen:
                continue
            seen.add(group)
            for counter in ("memory.events", "memory.current", "memory.max"):
                path = group / counter
                output.extend(f"=== {path} ===\n".encode())
                output.extend(read_counter(path))
                if len(output) > MAX_DUMP_BYTES:
                    return bytes(output[:MAX_DUMP_BYTES]) + b"\n[truncated]\n"
    return bytes(output)


def snapshot(directory, root_pid, stopped, report):
    directory.mkdir()
    processes = read_processes(stopped)
    if root_pid not in processes or processes[root_pid].identity != stopped.identity:
        report("ERROR: root exited or its PID was reused; collection stopped")
        return False
    scoped = descendants(processes, root_pid, os.getpid())
    tree = [processes[root_pid]] if root_pid in processes else []
    tree += sorted(scoped.values())
    (directory / "processes.txt").write_text(
        "PID PPID STATE START_ID EXECUTABLE\n" + "".join(
            f"{p.pid} {p.ppid} {p.state} {p.identity} {p.command}\n" for p in tree))
    jvms = [p for p in sorted(scoped.values()) if Path(p.command).name == "java"]
    complete = True
    if len(jvms) > MAX_JVMS:
        report(f"ERROR: {len(jvms)} JVMs found; capturing only the first {MAX_JVMS}")
        complete = False
    jvms = jvms[:MAX_JVMS]
    (directory / "memory.txt").write_bytes(memory_snapshot(
        [root_pid] + [process.pid for process in jvms], stopped))
    jcmd = shutil.which("jcmd")
    if not jcmd:
        report("ERROR: jcmd not found; install/use a full JDK and put its bin directory on PATH")
        return False
    if not jvms:
        report("ERROR: no live descendant JVMs found; inspect processes.txt and the root PID")
        return False
    for process in jvms:
        if stopped():
            return False
        processes = read_processes(stopped)
        if root_pid not in processes or processes[root_pid].identity != stopped.identity:
            report("ERROR: root exited or its PID was reused; collection stopped")
            return False
        current = descendants(processes, root_pid, os.getpid())
        if process.pid not in current or current[process.pid].identity != process.identity:
            report(f"ERROR: JVM {process.pid} exited or changed ancestry before attach; skipped")
            complete = False
            continue
        result = run_command(
            [jcmd, str(process.pid), "Thread.print", "-l"], 15, MAX_DUMP_BYTES, stopped)
        status = (f"\nwatchdog: returncode={result.returncode}; "
                  f"{result.error or 'completed'}\n").encode()
        (directory / f"jvm-{process.pid}.txt").write_bytes(result.output + status)
        if result.error:
            report(f"ERROR: jcmd PID {process.pid}: {result.error}; "
                   "inspect its dump file and verify JDK attach support/permissions")
            complete = False
    return complete


def positive_seconds(value):
    result = float(value)
    if not 0 < result <= 86400:
        raise argparse.ArgumentTypeError("must be greater than zero and at most 86400")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--root-pid", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--idle-seconds", type=positive_seconds, default=300)
    parser.add_argument("--snapshot-gap", type=positive_seconds, default=10)
    args = parser.parse_args()
    if args.root_pid <= 1 or args.root_pid == os.getpid():
        parser.error("--root-pid must identify the live shell owning the test command")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    def report(message):
        line = f"{datetime.now(timezone.utc).isoformat()} {message}\n"
        with (args.output_dir / "watchdog.log").open("a") as output:
            output.write(line)
        print("Spark test watchdog: " + message, file=sys.stderr, flush=True)

    stopped = Stop(args.root_pid)
    signal.signal(signal.SIGTERM, stopped.handle_signal)
    signal.signal(signal.SIGINT, stopped.handle_signal)
    previous = None
    last_activity = time.monotonic()
    report(f"Watching root PID {args.root_pid}; idle threshold {args.idle_seconds:g}s")
    try:
        while not stopped():
            try:
                stat = args.log.stat()
                signature = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
            except FileNotFoundError:
                signature = None
            if signature != previous:
                previous = signature
                last_activity = time.monotonic()
            if time.monotonic() - last_activity >= args.idle_seconds:
                report("Console idle; collecting one diagnostic pair (tests continue unchanged)")
                complete = snapshot(args.output_dir / "snapshot-1", args.root_pid, stopped, report)
                if stopped.wait(args.snapshot_gap):
                    complete = snapshot(
                        args.output_dir / "snapshot-2", args.root_pid, stopped, report) and complete
                else:
                    complete = False
                report("Diagnostic pair complete" if complete else "Diagnostics incomplete; see errors")
                while stopped.wait(1):
                    pass
                return 0 if complete else 1
            stopped.wait(min(1, args.idle_seconds))
        report("Stopped: root exited or watchdog received a stop signal")
        return 0
    except (OSError, RuntimeError) as error:
        report(f"ERROR: diagnostics failed: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

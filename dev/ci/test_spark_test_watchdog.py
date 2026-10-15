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

"""Bounded diagnostic and process-lifecycle tests; no Spark build required."""

import importlib.util
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).with_name("spark-test-watchdog.py").resolve()
SPEC = importlib.util.spec_from_file_location("watchdog", SCRIPT)
WATCHDOG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WATCHDOG)


class WatchdogTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="comet-spark-watchdog-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.log = self.directory / "console.log"
        self.log.touch()
        self.output = self.directory / "diagnostics"
        self.output.mkdir()
        self.console = (self.directory / "watchdog-console.log").open("w")
        self.addCleanup(self.console.close)

    def spawn(self, args, **kwargs):
        process = subprocess.Popen(args, **kwargs)
        self.addCleanup(self.cleanup_process, process)
        return process

    @staticmethod
    def cleanup_process(process):
        if process.poll() is None:
            process.kill()
        process.wait(timeout=3)

    def eventually(self, condition, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.02)
        self.fail("condition did not become true before timeout")

    def java_tree(self):
        # A harmless symlink to sleep exercises actual ps selection on
        # Linux and macOS without a JDK, Spark, or a JVM that needs terminating.
        java = self.directory / "java"
        java.symlink_to(shutil.which("sleep"))
        pid_file = self.directory / "java.pid"
        command = f"{shlex.quote(str(java))} 30 & echo $! > {shlex.quote(str(pid_file))}; wait"
        root = self.spawn(["/bin/sh", "-c", command])
        self.eventually(lambda: pid_file.exists() and pid_file.read_text().strip())
        java_pid = int(pid_file.read_text())

        def cleanup_java():
            try:
                os.kill(java_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        self.addCleanup(cleanup_java)
        return root, java_pid

    def fake_jcmd(self, body):
        bin_dir = self.directory / "bin"
        bin_dir.mkdir(exist_ok=True)
        script = bin_dir / "jcmd"
        script.write_text(f"#!{sys.executable}\n" + body)
        script.chmod(0o755)
        return {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"]}

    def start_watchdog(self, root, env=None, idle="0.1"):
        return self.spawn(
            [sys.executable, str(SCRIPT), "--root-pid", str(root.pid),
             "--log", str(self.log), "--output-dir", str(self.output),
             "--idle-seconds", idle, "--snapshot-gap", "0.1"],
            env=env, stdout=self.console, stderr=subprocess.STDOUT)

    def test_descendants_excludes_unrelated_and_watchdog_subtrees(self):
        def process(pid, ppid):
            return WATCHDOG.Process(pid, ppid, "S", str(pid), "java")
        processes = {pid: process(pid, ppid) for pid, ppid in [
            (10, 1), (11, 10), (12, 11), (20, 1), (21, 20), (30, 10), (31, 30)]}
        self.assertEqual(set(WATCHDOG.descendants(processes, 10, 30)), {11, 12})

    def snapshot_fixture(self, extra=1):
        processes = {10: WATCHDOG.Process(10, 1, "S", "root-start", "/bin/sh")}
        processes.update({pid: WATCHDOG.Process(pid, 10, "S", str(pid), "/jdk/bin/java")
                          for pid in range(11, 11 + extra)})
        stopped = Mock(return_value=False)
        stopped.identity = "root-start"
        report = Mock()
        return processes, stopped, report

    def test_rechecks_ancestry_before_attaching(self):
        processes, stopped, report = self.snapshot_fixture()
        moved = {**processes, 11: processes[11]._replace(ppid=99)}
        with patch.object(WATCHDOG, "read_processes", side_effect=[processes, moved]), \
                patch.object(WATCHDOG, "memory_snapshot", return_value=b"memory"), \
                patch.object(WATCHDOG.shutil, "which", return_value="jcmd"), \
                patch.object(WATCHDOG, "run_command") as run:
            self.assertFalse(WATCHDOG.snapshot(self.output / "one", 10, stopped, report))
        run.assert_not_called()
        self.assertIn("ancestry", report.call_args.args[0])

    def test_rechecks_jvm_identity_before_attaching(self):
        processes, stopped, report = self.snapshot_fixture()
        reused = {**processes, 11: processes[11]._replace(identity="new-start")}
        with patch.object(WATCHDOG, "read_processes", side_effect=[processes, reused]), \
                patch.object(WATCHDOG, "memory_snapshot", return_value=b"memory"), \
                patch.object(WATCHDOG.shutil, "which", return_value="jcmd"), \
                patch.object(WATCHDOG, "run_command") as run:
            self.assertFalse(WATCHDOG.snapshot(self.output / "one", 10, stopped, report))
        run.assert_not_called()

    def test_reused_root_pid_prevents_all_collection(self):
        processes, stopped, report = self.snapshot_fixture()
        processes[10] = processes[10]._replace(identity="different-root")
        with patch.object(WATCHDOG, "read_processes", return_value=processes), \
                patch.object(WATCHDOG, "run_command") as run:
            self.assertFalse(WATCHDOG.snapshot(self.output / "one", 10, stopped, report))
        run.assert_not_called()
        self.assertIn("reused", report.call_args.args[0])

    def test_missing_jcmd_is_an_actionable_diagnostic_failure(self):
        processes, stopped, report = self.snapshot_fixture()
        with patch.object(WATCHDOG, "read_processes", return_value=processes), \
                patch.object(WATCHDOG, "memory_snapshot", return_value=b"memory"), \
                patch.object(WATCHDOG.shutil, "which", return_value=None):
            self.assertFalse(WATCHDOG.snapshot(self.output / "one", 10, stopped, report))
        self.assertIn("full JDK", report.call_args.args[0])

    def test_failed_attach_records_stderr_and_failure_status(self):
        processes, stopped, report = self.snapshot_fixture()
        failure = WATCHDOG.CommandResult(b"AttachNotSupportedException", 1, "status 1")
        with patch.object(WATCHDOG, "read_processes", return_value=processes), \
                patch.object(WATCHDOG, "memory_snapshot", return_value=b"memory"), \
                patch.object(WATCHDOG.shutil, "which", return_value="jcmd"), \
                patch.object(WATCHDOG, "run_command", return_value=failure):
            self.assertFalse(WATCHDOG.snapshot(self.output / "one", 10, stopped, report))
        dump = (self.output / "one" / "jvm-11.txt").read_text()
        self.assertIn("AttachNotSupportedException", dump)
        self.assertIn("returncode=1", dump)
        self.assertIn("permissions", report.call_args.args[0])

    def test_limits_number_of_jvms(self):
        processes, stopped, report = self.snapshot_fixture(WATCHDOG.MAX_JVMS + 3)
        success = WATCHDOG.CommandResult(b"threads", 0, "")
        with patch.object(WATCHDOG, "read_processes", return_value=processes), \
                patch.object(WATCHDOG, "memory_snapshot", return_value=b"memory"), \
                patch.object(WATCHDOG.shutil, "which", return_value="jcmd"), \
                patch.object(WATCHDOG, "run_command", return_value=success) as run:
            self.assertFalse(WATCHDOG.snapshot(self.output / "one", 10, stopped, report))
        self.assertEqual(run.call_count, WATCHDOG.MAX_JVMS)
        for call in run.call_args_list:
            self.assertEqual(call.args[0][2:], ["Thread.print", "-l"])
            self.assertEqual(call.args[1:3], (15, WATCHDOG.MAX_DUMP_BYTES))

    def test_command_output_is_bounded(self):
        started = time.monotonic()
        result = WATCHDOG.run_command(
            [sys.executable, "-c", "import os,time; os.write(1,b'x'*100000); time.sleep(30)"],
            10, 1024, lambda: False)
        self.assertEqual(len(result.output), 1024)
        self.assertIn("exceeded", result.error)
        self.assertLess(time.monotonic() - started, 2)

    def test_command_timeout_is_bounded(self):
        started = time.monotonic()
        result = WATCHDOG.run_command(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            0.15, 1024, lambda: False)
        self.assertIn("timed out", result.error)
        self.assertIsNotNone(result.returncode)
        self.assertLess(time.monotonic() - started, 2)

    def test_command_failure_and_missing_executable_are_reported(self):
        result = WATCHDOG.run_command(
            [sys.executable, "-c", "import sys; print('failure'); sys.exit(23)"],
            2, 1024, lambda: False)
        self.assertEqual(result.returncode, 23)
        self.assertIn(b"failure", result.output)
        self.assertIn("23", result.error)
        missing = WATCHDOG.run_command([str(self.directory / "missing")], 2, 1024, lambda: False)
        self.assertIn("cannot start", missing.error)

    def test_counter_read_is_bounded(self):
        counter = self.directory / "counter"
        counter.write_bytes(b"x" * (WATCHDOG.MAX_COUNTER_BYTES + 100))
        data = WATCHDOG.read_counter(counter)
        self.assertLessEqual(len(data), WATCHDOG.MAX_COUNTER_BYTES + 20)
        self.assertIn(b"truncated", data)

    def test_linux_identity_uses_start_ticks_and_rejects_zombies(self):
        fields = ["S", "1"] + ["0"] * 17 + ["123456"]
        stat = "42 (name with spaces) " + " ".join(fields)
        with patch.object(WATCHDOG.sys, "platform", "linux"), \
                patch.object(Path, "read_text", return_value=stat):
            self.assertEqual(WATCHDOG.process_identity(42), "proc:123456")
        with patch.object(WATCHDOG.sys, "platform", "linux"), \
                patch.object(Path, "read_text", return_value=stat.replace(") S ", ") Z ")):
            self.assertIsNone(WATCHDOG.process_identity(42))

    def test_linux_memory_reads_selected_cgroup_v2_counters_once(self):
        def read(path):
            if path.name == "cgroup":
                return b"0::/test-group\n"
            return b"fixture counter\n"
        with patch.object(WATCHDOG.sys, "platform", "linux"), \
                patch.object(WATCHDOG, "read_counter", side_effect=read) as reader:
            output = WATCHDOG.memory_snapshot([10, 11], lambda: False)
        self.assertIn(b"/proc/meminfo", output)
        for counter in ("memory.events", "memory.current", "memory.max"):
            self.assertIn(f"/sys/fs/cgroup/test-group/{counter}".encode(), output)
            self.assertEqual(reader.call_args_list.count(
                unittest.mock.call(Path("/sys/fs/cgroup/test-group") / counter)), 1)
        self.assertEqual({str(call.args[0]) for call in reader.call_args_list
                          if call.args[0].name == "cgroup"}, {"/proc/10/cgroup", "/proc/11/cgroup"})

    def test_two_snapshots_then_wait_quietly_without_changing_target(self):
        root, java_pid = self.java_tree()
        calls = self.directory / "jcmd-calls"
        env = self.fake_jcmd(
            "import sys\nfrom pathlib import Path\n"
            f"with Path({str(calls)!r}).open('a') as out: out.write(' '.join(sys.argv[1:])+'\\n')\n"
            "print('fixture thread dump')\n")
        watcher = self.start_watchdog(root, env)
        self.eventually(lambda: (self.output / "snapshot-2" / f"jvm-{java_pid}.txt").exists())
        time.sleep(0.3)
        self.assertIsNone(watcher.poll(), "keep the PID reserved until caller cleanup")
        self.assertIsNone(root.poll())
        self.assertIsNotNone(WATCHDOG.process_identity(java_pid))
        self.assertEqual(calls.read_text().splitlines(), [f"{java_pid} Thread.print -l"] * 2)
        self.assertEqual(len(list(self.output.glob("snapshot-*"))), 2)
        watcher.terminate()
        self.assertEqual(watcher.wait(timeout=3), 0)
        self.assertIsNotNone(WATCHDOG.process_identity(java_pid))

    def test_log_activity_postpones_collection(self):
        root = self.spawn(["sleep", "30"])
        watcher = self.start_watchdog(root, idle="0.35")
        self.eventually(lambda: (self.output / "watchdog.log").exists())
        for _ in range(7):
            with self.log.open("a") as output:
                output.write("test running\n")
            time.sleep(0.1)
        self.assertFalse((self.output / "snapshot-1").exists())
        watcher.terminate()
        self.assertEqual(watcher.wait(timeout=3), 0)

    def assert_active_attach_cleanup(self, exit_parent):
        root, java_pid = self.java_tree()
        attach_pid_file = self.directory / "attach.pid"
        child_pid_file = self.directory / "attach-child.pid"
        env = self.fake_jcmd(
            "import os,signal,subprocess,sys,time\nfrom pathlib import Path\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
            f"Path({str(attach_pid_file)!r}).write_text(str(os.getpid()))\n"
            f"Path({str(child_pid_file)!r}).write_text(str(child.pid))\n"
            "time.sleep(30)\n")
        watcher = self.start_watchdog(root, env)
        self.eventually(lambda: child_pid_file.exists() and child_pid_file.read_text().strip())
        attach_pid = int(attach_pid_file.read_text())
        child_pid = int(child_pid_file.read_text())
        started = time.monotonic()
        if exit_parent:
            root.terminate()
            root.wait(timeout=3)
        else:
            watcher.terminate()
        self.assertEqual(watcher.wait(timeout=3), 1)
        self.assertLess(time.monotonic() - started, 3)
        self.eventually(lambda: WATCHDOG.process_identity(attach_pid) is None)
        self.eventually(lambda: WATCHDOG.process_identity(child_pid) is None)
        self.assertIsNotNone(WATCHDOG.process_identity(java_pid), "test target must survive")
        if not exit_parent:
            self.assertIsNone(root.poll())

    def test_term_cleans_up_active_diagnostic_group_only(self):
        self.assert_active_attach_cleanup(exit_parent=False)

    def test_parent_exit_cleans_up_active_diagnostic_group_only(self):
        self.assert_active_attach_cleanup(exit_parent=True)

    def test_parent_exit_before_idle_exits_promptly(self):
        root = self.spawn(["sleep", "30"])
        watcher = self.start_watchdog(root, idle="300")
        self.eventually(lambda: (self.output / "watchdog.log").exists())
        root.terminate()
        root.wait(timeout=3)
        self.assertEqual(watcher.wait(timeout=3), 0)
        self.assertFalse((self.output / "snapshot-1").exists())

    def test_cleanup_trap_preserves_test_exit_code(self):
        script = (
            f"{shlex.quote(sys.executable)} {shlex.quote(str(SCRIPT))} --root-pid $$ "
            f"--log {shlex.quote(str(self.log))} --output-dir {shlex.quote(str(self.output))} &\n"
            "watchdog_pid=$!\n"
            "trap 'status=$?; kill \"$watchdog_pid\" 2>/dev/null || true; "
            "wait \"$watchdog_pid\" 2>/dev/null || true; exit \"$status\"' EXIT\n"
            "sleep 0.2\nexit 37\n")
        result = subprocess.run(["/bin/sh", "-c", script], stdout=self.console,
                                stderr=subprocess.STDOUT, timeout=3, check=False)
        self.assertEqual(result.returncode, 37)


if __name__ == "__main__":
    unittest.main()

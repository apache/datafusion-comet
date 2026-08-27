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

"""Offline regression tests for the actual CI dependency retry script."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest


RETRY = Path(__file__).with_name("retry-download.sh")
RESOLVE_SPARK = Path(__file__).with_name("resolve-spark-dependencies.sh")
REPO = Path(__file__).resolve().parents[2]


class DownloadRetryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="comet-download-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calls = self.root / "calls.jsonl"
        self.delays = self.root / "delays"
        self.command = self.root / "download.py"
        self.command.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, sys\n"
            "calls = pathlib.Path(os.environ['COMET_TEST_CALLS'])\n"
            "attempt = len(calls.read_text().splitlines()) if calls.exists() else 0\n"
            "with calls.open('a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "failures = int(os.environ['COMET_TEST_FAILURES'])\n"
            "print('download output', flush=True)\n"
            "if attempt < failures:\n"
            "    print(os.environ['COMET_TEST_ERROR'], file=sys.stderr)\n"
            "    sys.exit(int(os.environ['COMET_TEST_STATUS']))\n"
        )
        self.command.chmod(0o755)
        (self.root / "build").mkdir()
        (self.root / "build" / "sbt").symlink_to(self.command)
        sleeper = self.root / "sleep"
        sleeper.write_text(
            '#!/bin/sh\nprintf "%s\\n" "$1" >> "$COMET_TEST_DELAYS"\n'
            'exit "${COMET_TEST_SLEEP_STATUS:-0}"\n'
        )
        sleeper.chmod(0o755)

    def run_download(
        self, message="Connection reset", failures=0, status=17,
        args=(), projects=None, sleep_status=0
    ):
        env = dict(
            os.environ,
            PATH=f"{self.root}{os.pathsep}{os.environ['PATH']}",
            COMET_TEST_CALLS=str(self.calls),
            COMET_TEST_DELAYS=str(self.delays),
            COMET_TEST_ERROR=message,
            COMET_TEST_FAILURES=str(failures),
            COMET_TEST_STATUS=str(status),
            COMET_TEST_SLEEP_STATUS=str(sleep_status),
        )
        command = ["bash", str(RETRY), sys.executable, str(self.command), *args]
        if projects is not None:
            command = ["bash", str(RESOLVE_SPARK), *projects]
        result = subprocess.run(
            command,
            cwd=self.root,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        calls = (
            [json.loads(line) for line in self.calls.read_text().splitlines()]
            if self.calls.exists() else []
        )
        delays = (
            [int(line) for line in self.delays.read_text().splitlines()]
            if self.delays.exists() else []
        )
        return result, calls, delays

    def assert_backoff(self, delays):
        for index, delay in enumerate(delays):
            self.assertIn(delay, range(10 * 2**index, 10 * 2**index + 5))

    def test_success_does_not_retry_and_preserves_arguments(self):
        args = ("one argument", "*", "-Dkey=literal $value")
        result, calls, delays = self.run_download(args=args)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(calls, [list(args)])
        self.assertEqual(delays, [])
        self.assertIn("download output", result.stdout)

    def test_transient_download_errors_retry(self):
        messages = (
            "Could not transfer artifact: Connection reset",
            "Server returned HTTP response code: 502 for URL: https://repo.invalid/a.jar",
            "status code: 429, reason phrase: Too Many Requests (429)",
            "status code: 500, reason phrase: Internal Server Error (500)",
            "HTTP/1.1 503 Service Unavailable",
            "curl: (22) The requested URL returned error: 504",
            "java.net.SocketTimeoutException: Read timed out",
            "Network is unreachable (os error 101)",
        )
        for message in messages:
            with self.subTest(message=message):
                self.calls.unlink(missing_ok=True)
                self.delays.unlink(missing_ok=True)
                result, calls, delays = self.run_download(message, failures=2)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(len(calls), 3)
                self.assertEqual(len(delays), 2)
                self.assert_backoff(delays)
                self.assertIn(message, result.stdout)

    def test_fourth_attempt_can_succeed(self):
        result, calls, delays = self.run_download(failures=3)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(calls), 4)
        self.assertEqual(len(delays), 3)
        self.assert_backoff(delays)

    def test_exhaustion_preserves_exit_code_without_extra_sleep(self):
        result, calls, delays = self.run_download(failures=10, status=23)
        self.assertEqual(result.returncode, 23)
        self.assertEqual(len(calls), 4)
        self.assertEqual(len(delays), 3)
        self.assertIn("failed after 4 attempts", result.stdout)

    def test_permanent_failures_do_not_retry(self):
        for message in (
            "status code: 404, reason phrase: Not Found (404)",
            "status code: 401, reason phrase: Unauthorized (401)",
            "[error] Could not find artifact missing:dependency:jar:1.0",
            "[error] not found: value invalidBuildSetting",
            "[ERROR] COMPILATION ERROR",
            "[ERROR] There are test failures.",
        ):
            with self.subTest(message=message):
                self.calls.unlink(missing_ok=True)
                self.delays.unlink(missing_ok=True)
                result, calls, delays = self.run_download(message, failures=10, status=42)
                self.assertEqual(result.returncode, 42)
                self.assertEqual(len(calls), 1)
                self.assertEqual(delays, [])

    def test_signal_exit_does_not_retry_even_after_network_message(self):
        for status in (130, 137, 139, 143):
            with self.subTest(status=status):
                self.calls.unlink(missing_ok=True)
                self.delays.unlink(missing_ok=True)
                result, calls, delays = self.run_download(failures=10, status=status)
                self.assertEqual(result.returncode, status)
                self.assertEqual(len(calls), 1)
                self.assertEqual(delays, [])

    def test_missing_command_fails(self):
        result = subprocess.run(["bash", str(RETRY)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("Usage:", result.stderr)

    def test_interrupted_backoff_does_not_start_another_attempt(self):
        result, calls, delays = self.run_download(failures=1, sleep_status=143)
        self.assertEqual(result.returncode, 143)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(delays), 1)

    def test_spark_retries_dependency_tasks_only(self):
        result, calls, delays = self.run_download(
            "Server returned HTTP response code: 502", failures=2, projects=("sql", "hive")
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        expected = [
            "-batch", "-Dsbt.log.noformat=true", "-mem", "1024",
            "sql/Test/update", "hive/Test/update"
        ]
        self.assertEqual(calls, [expected] * 3)
        self.assertEqual(len(delays), 2)
        self.assert_backoff(delays)

    def test_spark_build_definition_failure_does_not_retry(self):
        result, calls, delays = self.run_download(
            "[error] not found: value invalidBuildSetting", failures=10, projects=("catalyst",)
        )
        self.assertEqual(result.returncode, 17)
        self.assertEqual(len(calls), 1)
        self.assertEqual(delays, [])

    def test_spark_rejects_commands_outside_dependency_resolution(self):
        for projects in ((), ("sql/test",), ("sql", "hive/Test/compile")):
            with self.subTest(projects=projects):
                result, calls, delays = self.run_download(projects=projects)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(calls, [])
                self.assertEqual(delays, [])


@unittest.skipUnless(
    os.environ.get("COMET_TEST_MAVEN_DOWNLOADS") == "1",
    "requires Maven bootstrapped/configured by the setup-maven action",
)
class MavenTransferRetryTest(unittest.TestCase):
    """Exercise CI's actual MAVEN_OPTS with Maven, not a simulated resolver.

    The local mirror serves a parent POM. `validate` needs no plugins, does not
    compile/run tests, and all artifact requests stay on this loopback server.
    Preflight already bootstrapped Maven; only shorten the retry delay here.
    """

    def test_transfer_retries_are_bounded_and_permanent_failures_still_fail(self):
        parent = (
            '<project xmlns="http://maven.apache.org/POM/4.0.0">'
            '<modelVersion>4.0.0</modelVersion><groupId>comet.ci</groupId>'
            '<artifactId>parent</artifactId><version>1</version>'
            '<packaging>pom</packaging></project>'
        ).encode()
        state = {}

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                if self.path.endswith(".pom"):
                    state["requests"] += 1
                    if state["requests"] <= state["failures"]:
                        self.send_response(state["status"])
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    body = parent
                elif self.path.endswith(".sha1"):
                    body = hashlib.sha1(parent).hexdigest().encode()
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(server.server_close)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory(prefix="comet-maven-test-") as directory:
            root = Path(directory)
            (root / "pom.xml").write_text(
                '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                '<modelVersion>4.0.0</modelVersion><parent><groupId>comet.ci</groupId>'
                '<artifactId>parent</artifactId><version>1</version><relativePath/>'
                '</parent><artifactId>child</artifactId><packaging>pom</packaging></project>'
            )
            (root / "settings.xml").write_text(
                '<settings><mirrors><mirror><id>test</id><mirrorOf>*</mirrorOf>'
                f'<url>http://127.0.0.1:{server.server_port}</url>'
                '</mirror></mirrors></settings>'
            )
            env = dict(os.environ)
            env["MAVEN_OPTS"] = (
                env.get("MAVEN_OPTS", "")
                + " -Daether.connector.http.retryHandler.interval=100"
            )
            for status, failures, expected_requests, succeeds in (
                (429, 2, 3, True), (502, 2, 3, True),
                (503, 10, 4, False), (404, 10, 1, False),
            ):
                with self.subTest(status=status):
                    state.update(status=status, failures=failures, requests=0)
                    result = subprocess.run(
                        [
                            str(REPO / "mvnw"), "-B", "-ntp",
                            "-f", str(root / "pom.xml"),
                            "-s", str(root / "settings.xml"),
                            f"-Dmaven.repo.local={root / str(status)}", "validate",
                        ],
                        cwd=REPO, env=env, capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(
                        result.returncode == 0, succeeds,
                        result.stdout + result.stderr,
                    )
                    self.assertEqual(state["requests"], expected_requests)


if __name__ == "__main__":
    unittest.main()

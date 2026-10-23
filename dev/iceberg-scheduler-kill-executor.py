#!/usr/bin/env python3
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

"""Linux/SSH harness: kill the exact Spark executor PID supplied by the test probe."""

import re
import subprocess
import sys


def main():
    if len(sys.argv) != 5:
        raise SystemExit("usage: kill-executor HOST PID EXECUTOR_ID RUN_DIRECTORY")
    host, pid, executor_id, _ = sys.argv[1:]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]*", host):
        raise SystemExit("invalid executor host")
    if not pid.isdigit() or int(pid) <= 1 or not executor_id.isdigit():
        raise SystemExit("invalid executor PID or ID")
    # Check identity on the target host immediately before SIGKILL. Never kill a worker,
    # driver, an arbitrary Java PID, or a process selected by a filename prefix.
    remote = r'''
import os
from pathlib import Path
import signal
import sys
import time
pid, executor_id = int(sys.argv[1]), sys.argv[2]
proc = Path("/proc") / str(pid)
args = proc.joinpath("cmdline").read_bytes().decode().split("\0")
if not any(a.endswith(".CoarseGrainedExecutorBackend") for a in args):
    raise SystemExit("PID is not a Spark executor: " + repr(args))
if "--executor-id" not in args or args[args.index("--executor-id") + 1] != executor_id:
    raise SystemExit("executor ID does not match PID")
print("SIGKILL executor=" + executor_id + " pid=" + str(pid), flush=True)
os.kill(pid, signal.SIGKILL)
deadline = time.monotonic() + 20
while proc.exists():
    try:
        state = proc.joinpath("stat").read_text().split(")", 1)[1].split()[0]
        if state == "Z":
            break
    except FileNotFoundError:
        break
    if time.monotonic() >= deadline:
        raise SystemExit("executor process did not exit")
    time.sleep(0.05)
print("Executor terminated", flush=True)
'''
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host,
         "python3", "-", pid, executor_id],
        input=remote, text=True, timeout=28, check=False)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()

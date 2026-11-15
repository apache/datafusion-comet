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

"""Regression tests for dev/ci/fork-push-changed-files.sh, on real git repositories."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("fork-push-changed-files.sh").resolve()
GIT_ENV = {
    "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
    "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com",
    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull,
}


def git(cwd, *args):
    result = subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
        env={**os.environ, **GIT_ENV})
    return result.stdout.strip()


def write(repo, path, text):
    file = Path(repo, path)
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(text)


def commit(repo, message, files):
    for path, text in files.items():
        write(repo, path, text)
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", message)
    return git(repo, "rev-parse", "HEAD")


class ForkPushChangedFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.upstream = root / "upstream"
        self.fork = root / "fork"
        git(root, "init", "-q", "-b", "main", str(self.upstream))
        commit(self.upstream, "base", {"README.md": "a\n", "native/lib.rs": "1\n", "spark/Plan.scala": "1\n"})
        git(root, "clone", "-q", str(self.upstream), str(self.fork))
        git(self.fork, "checkout", "-q", "-b", "work")

    def changed(self, head=None, url=None):
        head = head or git(self.fork, "rev-parse", "HEAD")
        result = subprocess.run(
            [str(SCRIPT), head], cwd=self.fork, check=True, capture_output=True, text=True,
            env={**os.environ, **GIT_ENV, "UPSTREAM_URL": url or str(self.upstream)})
        return set(result.stdout.split())

    def test_lists_what_the_branch_changed(self):
        commit(self.fork, "native", {"native/lib.rs": "2\n"})
        self.assertEqual(self.changed(), {"native/lib.rs"})

    def test_docs_only_branch_selects_no_code(self):
        commit(self.fork, "docs", {"README.md": "b\n"})
        self.assertEqual(self.changed(), {"README.md"})

    def test_revert_of_newer_upstream_code_is_kept_when_the_fork_is_stale(self):
        # The fork's main stays at the base while upstream moves on and changes a
        # native file. A branch cut from the newer upstream that reverts the file
        # matches the fork's stale main, so a diff against it would omit the file.
        commit(self.upstream, "upstream native change", {"native/lib.rs": "2\n"})
        # Fetch by URL so the fork's own origin/main stays at the base.
        git(self.fork, "fetch", "-q", str(self.upstream), "main")
        git(self.fork, "checkout", "-q", "-b", "revert", "FETCH_HEAD")
        commit(self.fork, "revert", {"native/lib.rs": "1\n", "README.md": "b\n"})
        self.assertEqual(self.changed(), {"native/lib.rs", "README.md"})

    def test_commits_made_on_the_fork_main_are_kept_when_pushed_to_a_branch(self):
        # `git push fork main:my-fix`: the fork's main holds commits upstream does
        # not have, and the pull request from the branch will show them.
        commit(self.fork, "native", {"native/lib.rs": "2\n"})
        git(self.fork, "checkout", "-q", "main")
        git(self.fork, "merge", "-q", "--ff-only", "work")
        self.assertEqual(self.changed(), {"native/lib.rs"})

    def test_code_then_docs_commits_keep_the_code_change(self):
        commit(self.fork, "native", {"native/lib.rs": "2\n"})
        commit(self.fork, "docs", {"README.md": "b\n"})
        self.assertEqual(self.changed(), {"native/lib.rs", "README.md"})

    def test_an_unreachable_upstream_runs_everything(self):
        commit(self.fork, "docs", {"README.md": "b\n"})
        self.assertEqual(
            self.changed(url=str(Path(self.tmp.name, "missing"))),
            {"README.md", "native/lib.rs", "spark/Plan.scala"})

    def test_a_branch_that_adds_nothing_to_upstream_selects_nothing(self):
        # A branch pushed before its first commit: the pull request will show no
        # change, so there is nothing to test.
        self.assertEqual(self.changed(), set())


if __name__ == "__main__":
    unittest.main()

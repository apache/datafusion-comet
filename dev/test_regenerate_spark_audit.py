#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "regenerate_spark_audit",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "regenerate-spark-audit.py"),
)
rsa = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rsa)


class ParseExistingTest(unittest.TestCase):
    def test_parses_marker_block(self):
        body = (
            "preamble\n"
            "<!-- BEGIN AUDIT LIST -->\n"
            "- `abcd1234` 2026-01-15 [needs-triage] SPARK-1: Foo\n"
            "- `def56789` 2026-01-16 [relevant] SPARK-2: Bar. comet#5023\n"
            "<!-- END AUDIT LIST -->\n"
            "trailer\n"
        )
        result = rsa.parse_existing_block(body)
        self.assertEqual(set(result.keys()), {"abcd1234", "def56789"})
        self.assertIn("[needs-triage]", result["abcd1234"])
        self.assertIn("[relevant]", result["def56789"])
        self.assertIn("comet#5023", result["def56789"])

    def test_handles_empty_block(self):
        body = (
            "<!-- BEGIN AUDIT LIST -->\n"
            "<!-- END AUDIT LIST -->\n"
        )
        self.assertEqual(rsa.parse_existing_block(body), {})

    def test_missing_markers_raises(self):
        with self.assertRaises(ValueError):
            rsa.parse_existing_block("no markers here")


class FormatNewLineTest(unittest.TestCase):
    def test_basic(self):
        line = rsa.format_new_line(
            short_hash="abcd1234",
            date="2026-01-15",
            subject="SPARK-1: Add foo",
        )
        self.assertEqual(
            line,
            "- `abcd1234` 2026-01-15 [needs-triage] SPARK-1: Add foo",
        )

    def test_truncates_long_subject(self):
        long_subject = "A" * 300
        line = rsa.format_new_line(
            short_hash="abcd1234",
            date="2026-01-15",
            subject=long_subject,
        )
        self.assertLessEqual(len(line), 250)
        self.assertTrue(line.endswith("..."))


class IsInScopeTest(unittest.TestCase):
    def test_pure_sql_core(self):
        self.assertTrue(rsa.is_in_scope(["sql/core/src/main/scala/Foo.scala"]))

    def test_pure_connect(self):
        self.assertFalse(rsa.is_in_scope(["sql/connect/server/src/Bar.scala"]))

    def test_pure_thriftserver(self):
        self.assertFalse(rsa.is_in_scope(["sql/hive-thriftserver/src/Baz.scala"]))

    def test_mixed_sql_and_connect(self):
        self.assertTrue(
            rsa.is_in_scope(
                [
                    "sql/connect/server/src/Bar.scala",
                    "sql/catalyst/src/main/scala/Qux.scala",
                ]
            )
        )

    def test_no_sql_paths(self):
        self.assertFalse(rsa.is_in_scope(["core/src/main/scala/Z.scala"]))

    def test_empty(self):
        self.assertFalse(rsa.is_in_scope([]))


class MergeTest(unittest.TestCase):
    def test_preserves_existing_and_appends_new(self):
        existing = {
            "abcd1234": "- `abcd1234` 2026-01-15 [relevant] SPARK-1: Foo. comet#5023",
        }
        commits = [
            {"short": "abcd1234", "date": "2026-01-15", "subject": "SPARK-1: Foo"},
            {"short": "def56789", "date": "2026-01-16", "subject": "SPARK-2: Bar"},
        ]
        result = rsa.merge_lines(commits, existing)
        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0],
            "- `abcd1234` 2026-01-15 [relevant] SPARK-1: Foo. comet#5023",
        )
        self.assertEqual(
            result[1],
            "- `def56789` 2026-01-16 [needs-triage] SPARK-2: Bar",
        )

    def test_chronological_order(self):
        existing: dict[str, str] = {}
        commits = [
            {"short": "11111111", "date": "2026-01-01", "subject": "X"},
            {"short": "22222222", "date": "2026-01-02", "subject": "Y"},
            {"short": "33333333", "date": "2026-01-03", "subject": "Z"},
        ]
        result = rsa.merge_lines(commits, existing)
        self.assertEqual(len(result), 3)
        self.assertIn("11111111", result[0])
        self.assertIn("22222222", result[1])
        self.assertIn("33333333", result[2])


class WriteBlockTest(unittest.TestCase):
    def test_replaces_block_only(self):
        original = (
            "preamble line\n"
            "<!-- BEGIN AUDIT LIST -->\n"
            "- `abcd1234` 2026-01-15 [needs-triage] OLD\n"
            "<!-- END AUDIT LIST -->\n"
            "trailer line\n"
        )
        new_lines = [
            "- `abcd1234` 2026-01-15 [relevant] NEW. comet#9",
            "- `def56789` 2026-01-16 [needs-triage] FRESH",
        ]
        result = rsa.replace_block(original, new_lines)
        self.assertIn("preamble line", result)
        self.assertIn("trailer line", result)
        self.assertIn("[relevant] NEW. comet#9", result)
        self.assertIn("[needs-triage] FRESH", result)
        self.assertNotIn("OLD", result)
        self.assertIn(rsa.BEGIN_MARKER, result)
        self.assertIn(rsa.END_MARKER, result)

    def test_empty_lines_yields_empty_block(self):
        original = (
            "<!-- BEGIN AUDIT LIST -->\n"
            "- `abcd1234` 2026-01-15 [needs-triage] OLD\n"
            "<!-- END AUDIT LIST -->\n"
        )
        result = rsa.replace_block(original, [])
        self.assertNotIn("OLD", result)
        self.assertIn(rsa.BEGIN_MARKER, result)
        self.assertIn(rsa.END_MARKER, result)


if __name__ == "__main__":
    unittest.main()

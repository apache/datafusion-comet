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


if __name__ == "__main__":
    unittest.main()

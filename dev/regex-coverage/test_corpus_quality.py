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

import unittest

from corpus_quality import quality


class CorpusQualityTests(unittest.TestCase):
    def test_population_separation_and_eligible_diversity(self):
        sources = [
            {"id": "app", "population": "application", "repository": "a/app",
             "domain": "genomics", "independence_group": "a"},
            {"id": "dynamic", "population": "application", "repository": "b/app",
             "domain": "physics", "independence_group": "b"},
            {"id": "tool", "population": "toolkit", "repository": "c/tool",
             "domain": "quality", "independence_group": "c"},
            {"id": "engine", "population": "engine_fixture", "repository": "d/engine",
             "domain": "sql", "independence_group": "d"},
        ]
        eligible = lambda source: {"source": source, "measurement_status": "eligible",
                                   "classification": "DIRECT_LITERAL", "pattern_value": "same"}
        rows = [eligible("app"), eligible("app"), eligible("tool"), eligible("engine"),
                {"source": "dynamic", "measurement_status": "not_a_direct_literal_candidate",
                 "classification": "DYNAMIC_PATTERN"}]
        targets = {"application_independence_groups": 3, "application_domains": 3,
                   "application_unique_eligible_patterns": 30}
        report = quality(rows, sources, [{"source": "tool"}], targets)
        app = report["populations"]["application"]
        self.assertEqual(app["inspected_repositories"], 2)
        self.assertEqual(app["eligible_independence_groups"], ["a"])
        self.assertEqual(app["eligible_domains"], ["genomics"])
        self.assertEqual(app["eligible_occurrences"], 2)
        self.assertEqual(app["unique_decoded_eligible_patterns"], 1)
        self.assertEqual(app["exclusion_percent"], 33.333333)
        self.assertEqual(report["populations"]["toolkit"]["non_rlike_audit_records"], 1)
        self.assertTrue(all(not check["met"] for check in report["collection_targets"].values()))
        self.assertFalse(report["representativeness_established"])
        self.assertIsNone(app["admission_rate_pooled"])

    def test_empty_population_has_no_zero_percent_rate(self):
        targets = {"application_independence_groups": 3, "application_domains": 3,
                   "application_unique_eligible_patterns": 30}
        report = quality([], [], [], targets)
        self.assertIsNone(report["populations"]["application"]["exclusion_percent"])


if __name__ == "__main__":
    unittest.main()

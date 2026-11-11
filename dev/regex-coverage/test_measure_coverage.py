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

from measure_coverage import prepare_requests, summarize


class MeasurementTests(unittest.TestCase):
    def test_occurrence_and_unique_denominators_exclude_invalid_patterns(self):
        sources = [{"id": "a", "category": "example"}, {"id": "b", "category": "example"}]
        rows = [{"source": "a", "measurement_status": "eligible",
                 "pattern_value": "x", "admitted": True} for _ in range(3)]
        rows += [{"source": "a", "measurement_status": "eligible",
                  "pattern_value": "^y", "admitted": False},
                 {"source": "a", "measurement_status": "invalid_java_regex"},
                 {"source": "b", "measurement_status": "not_validated"}]
        first, second = summarize(rows, sources)
        self.assertEqual((first["eligible_occurrences"], first["admitted_occurrences"],
                          first["rejected_occurrences"]), (4, 3, 1))
        self.assertEqual(first["occurrence_admission_percent"], 75.0)
        self.assertEqual(first["unique_admission_percent"], 50.0)
        self.assertEqual(first["excluded_occurrences"], {"invalid_java_regex": 1})
        self.assertIsNone(second["occurrence_admission_percent"])
        self.assertIsNone(second["unique_admission_percent"])

    def test_same_pattern_cannot_have_inconsistent_decisions(self):
        rows = [{"source": "a", "measurement_status": "eligible",
                 "pattern_value": "x", "admitted": value} for value in (True, False)]
        with self.assertRaises(ValueError):
            summarize(rows, [{"id": "a", "category": "example"}])

    def test_requests_preserve_values_and_require_explicit_source_policy(self):
        sources = [{"id": "py", "measurement": {"decoder": "python-value"}},
                   {"id": "sql", "measurement": {"decoder": "spark-literal"}},
                   {"id": "skip", "measurement": {"exclude": "unvalidated_templates"}}]
        rows = [{"source": "py", "status": "direct_literal_candidate", "pattern_value": r"\d",
                 "api_binding_verified": True},
                {"source": "sql", "status": "direct_literal_candidate", "literal_sql": r"'\\d'"},
                {"source": "skip", "status": "direct_literal_candidate", "literal_sql": "'x'"},
                {"source": "py", "status": "non_direct_literal_or_unparsed"},
                {"source": "sql", "status": "direct_literal_candidate"}]
        requests, exclusions = prepare_requests(rows, sources)
        self.assertEqual(requests, [
            {"input_index": 0, "decoder": "python-value", "value": r"\d"},
            {"input_index": 1, "decoder": "spark-literal", "value": r"'\\d'"}])
        self.assertEqual(exclusions, {2: "unvalidated_templates",
                                      3: "not_a_direct_literal_candidate",
                                      4: "missing_literal_value"})

    def test_sensitivity_separates_invalid_legacy_patterns(self):
        rows = [{"source": "a", "measurement_status": "eligible", "pattern_value": "x",
                 "admitted": True, "spark_escaped_literals_true": {
                     "decoding_differs": True, "java_valid": False}},
                {"source": "a", "measurement_status": "eligible", "pattern_value": "y",
                 "admitted": True, "spark_escaped_literals_true": {
                     "decoding_differs": True, "java_valid": True, "admitted": False}}]
        result = summarize(rows, [{"id": "a", "category": "example"}])[0]
        self.assertEqual(result["spark_legacy_decoding_changes"], 2)
        self.assertEqual(result["spark_legacy_admission_changes"], 1)
        self.assertEqual(result["spark_legacy_invalid_occurrences"], 1)

    def test_resolved_constants_need_source_policy_and_verified_api(self):
        row = {"source": "py", "status": "resolved_constant_candidate",
               "classification": "RESOLVED_CONSTANT", "pattern_value": "x",
               "constant_resolution_verified": True, "api_binding_verified": True}
        policy = {"id": "py", "measurement": {"decoder": "python-value"}}
        self.assertEqual(prepare_requests([row], [policy])[1], {0: "unvalidated_resolved_constant"})
        policy["measurement"]["allow_resolved_constants"] = True
        self.assertEqual(len(prepare_requests([row], [policy])[0]), 1)
        row["api_binding_verified"] = False
        self.assertEqual(prepare_requests([row], [policy])[1], {0: "unverified_python_api"})


if __name__ == "__main__":
    unittest.main()

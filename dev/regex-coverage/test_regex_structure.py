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

from analyze_coverage import distribution, source_summaries
from regex_structure import EXPERIMENTS, control_pattern, scan


class StructureTests(unittest.TestCase):
    def test_literal_dot_and_backslash_boundaries(self):
        for pattern in (r"[.]", r"[^.]", r"\\d", r"\^\.\$", r"[(?=]", r"\(\?="):
            with self.subTest(pattern=pattern):
                result = scan(pattern)
                self.assertTrue(result["complete"])
                self.assertEqual(result["features"], [])
                self.assertEqual(result["metrics"]["quantifiers"], 0)

    def test_shorthand_context_and_class_negation(self):
        result = scan(r"^[\d.]$")
        self.assertTrue(result["complete"])
        self.assertEqual(result["features"], ["ANCHOR_END", "ANCHOR_START", "SHORTHAND_DIGIT"])
        self.assertEqual(result["blockers"][1]["context"], "class")
        self.assertEqual(result["metrics"]["character_classes"], 1)

    def test_nested_groups_and_counted_quantifiers(self):
        result = scan(r"(?:[a-z]+|ab){2,4}")
        self.assertTrue(result["complete"])
        expected = {"groups": 1, "max_group_depth": 1, "quantifiers": 2,
                    "max_quantifier_nesting": 2, "max_counted_bound": 4,
                    "alternations": 1, "character_classes": 1}
        self.assertEqual({k: result["metrics"][k] for k in expected}, expected)
        result = scan("((a*)+)?")
        self.assertEqual(result["metrics"]["max_quantifier_nesting"], 3)
        self.assertEqual(result["metrics"]["max_group_depth"], 2)

    def test_controls_and_unicode_lengths(self):
        result = scan("^fo\no$")
        self.assertEqual(result["features"], ["ANCHOR_END", "ANCHOR_START", "CONTROL_CHARACTER"])
        result = scan("😀")
        self.assertEqual(result["metrics"]["length_codepoints"], 1)
        self.assertEqual(result["metrics"]["length_utf16"], 2)
        self.assertEqual(result["features"], ["NON_ASCII"])
        self.assertFalse(result["metrics"]["ascii_only"])

    def test_unknown_shapes_are_partial(self):
        for pattern in (r"foo(?=bar)", r"\Q.[a]^\E", r"[a-z&&[^x]]", r"[]]", "a+?", "(a"):
            with self.subTest(pattern=pattern):
                result = scan(pattern)
                self.assertFalse(result["complete"])
                self.assertIsNone(result["metrics"])
                self.assertTrue(result["limitation"])

    def test_structure_inventory_is_not_an_admission_classifier(self):
        result = scan("a{257}")
        self.assertTrue(result["complete"])
        self.assertEqual(result["features"], [])
        self.assertEqual(result["metrics"]["max_counted_bound"], 257)
        # The real analyzer rejects this shape, as the JVM component checks verify.
        self.assertNotIn("admitted", result)

    def test_syntax_controls_preserve_class_and_escape_context(self):
        value = r"^[.]\.a.$"
        structure = scan(value)
        self.assertEqual(control_pattern(value, structure["blockers"], EXPERIMENTS["both_anchor_types"]),
                         r"[.]\.a.")
        self.assertEqual(control_pattern(value, structure["blockers"], EXPERIMENTS["dot_wildcard"]),
                         r"^[.]\.aa$")
        self.assertEqual(control_pattern(value, structure["blockers"], EXPERIMENTS["all_observed_blockers"]),
                         r"[.]\.aa")

    def test_empty_pattern(self):
        result = scan("")
        self.assertTrue(result["complete"])
        self.assertEqual(result["metrics"]["length_codepoints"], 0)
        self.assertEqual(result["metrics"]["max_quantifier_nesting"], 0)

    def test_leading_anchor_control_does_not_remove_internal_anchors(self):
        value = "^a(^b)"
        result = control_pattern(value, scan(value)["blockers"],
                                 EXPERIMENTS["leading_anchor"], leading_only=True)
        self.assertEqual(result, "a(^b)")


class AnalysisSummaryTests(unittest.TestCase):
    def test_nearest_rank_percentiles_and_empty_denominator(self):
        self.assertEqual(distribution([0, 1, 2, 3, 4, 100]),
                         {"n": 6, "min": 0, "p50": 2, "p90": 100, "p99": 100, "max": 100})
        self.assertEqual(distribution([])["n"], 0)
        self.assertIsNone(distribution([])["p50"])

    def test_primary_counts_overlap_and_unique_patterns(self):
        rows = []
        for value, primary in (("^a$", "ANCHOR"), ("^a$", "ANCHOR"),
                               (r"\d", "SHORTHAND_CLASS"), ("a{257}", "UNATTRIBUTED")):
            rows.append({"source": "example", "pattern_value": value, "admitted": False,
                         "primary_reason": primary, "primary_verified": primary != "UNATTRIBUTED",
                         "structure": scan(value), "scanner_expansion": None, "controls": {}})
        summary = source_summaries(rows, [{"id": "example", "category": "test"}])[0]
        occurrences = summary["occurrences"]
        unique = summary["unique_patterns"]
        self.assertEqual(occurrences["rejected"], 4)
        self.assertEqual(sum(occurrences["primary_reasons"].values()), 4)
        self.assertEqual(sum(occurrences["unsupported_features_rejected"].values()), 5)
        self.assertEqual(occurrences["feature_pairs_rejected"], {"ANCHOR_END + ANCHOR_START": 2})
        self.assertEqual(occurrences["verified_primary_blockers"], 3)
        self.assertEqual(unique["rejected"], 3)
        self.assertEqual(unique["primary_reasons"]["ANCHOR"], 1)
        self.assertEqual(occurrences["metrics"]["scanner_expansion_admitted_only"]["n"], 0)


if __name__ == "__main__":
    unittest.main()

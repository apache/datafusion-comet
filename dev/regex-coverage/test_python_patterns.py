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

from python_patterns import inventory


IMPORT = "from pyspark.sql import functions as F\n"


class FunctionPatternTests(unittest.TestCase):
    def test_function_literals_aliases_and_keywords(self):
        rows = inventory(IMPORT + r'''from pyspark.sql.functions import rlike as rx, lit as literal
F.rlike(F.col("name"), F.lit(r"\d+"))
rx("name", literal("^[A-Z]+$"))
F.rlike(str="name", regexp=F.lit(col="[.]"))
''')
        self.assertEqual([r["pattern_value"] for r in rows], [r"\d+", "^[A-Z]+$", "[.]"])
        self.assertTrue(all(r["classification"] == "DIRECT_LITERAL" for r in rows))
        self.assertTrue(all(r["api_binding_verified"] for r in rows))

    def test_column_names_and_column_expressions_are_not_literals(self):
        rows = inventory(IMPORT + '''F.rlike("name", "regex")
F.rlike("name", F.col("regex"))
F.rlike("name", variable)
''')
        self.assertEqual([r["classification"] for r in rows], ["DYNAMIC_PATTERN"] * 3)
        self.assertTrue(all("pattern_value" not in r for r in rows))

    def test_null_numeric_and_bytes_literals_are_not_string_patterns(self):
        rows = inventory(IMPORT + '''F.rlike("name", F.lit(None))
F.rlike("name", F.lit(123))
F.rlike("name", F.lit(b"bytes"))
''')
        self.assertEqual([r["classification"] for r in rows], ["UNKNOWN"] * 3)

    def test_reassignment_and_parameter_shadowing_disable_proof(self):
        for text in ('''F = other
F.rlike("name", F.lit("x"))''', '''def run(F):
    return F.rlike("name", F.lit("x"))''', '''F.rlike = other
F.rlike("name", F.lit("x"))''', '''F.__dict__["lit"] = other
F.rlike("name", F.lit("x"))'''):
            with self.subTest(text=text):
                rows = inventory(IMPORT + text)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["classification"], "UNKNOWN")
                self.assertNotIn("pattern_value", rows[0])

    def test_shadowed_direct_import_is_not_silently_admitted(self):
        rows = inventory('''from pyspark.sql.functions import rlike as rx, lit
def run(rx):
    return rx("name", lit("x"))
''')
        self.assertEqual(rows[0]["classification"], "UNKNOWN")

    def test_only_exact_pyspark_function_symbols_are_verified(self):
        rows = inventory(IMPORT + 'F.custom.rlike("name", F.lit("x"))')
        self.assertEqual(rows[0]["classification"], "UNKNOWN")
        self.assertFalse(rows[0]["api_binding_verified"])
        self.assertNotIn("pattern_value", rows[0])

    def test_namespace_mutation_and_wildcard_import_disable_proof(self):
        for extra in ("from anything import *\n", "globals()[key] = value\n",
                      'setattr(F, "lit", other)\n'):
            with self.subTest(extra=extra):
                rows = inventory(IMPORT + extra + 'F.rlike("name", F.lit("x"))')
                self.assertEqual(rows[0]["classification"], "UNKNOWN")

    def test_literal_addition_and_single_assignment_constants(self):
        rows = inventory(IMPORT + '''START = "^[A"
END = "Z]+$"
F.rlike("name", F.lit(START + "-" + END))
F.rlike("name", F.lit("^" + "[a-z]+" + "$"))
''')
        self.assertEqual([r["pattern_value"] for r in rows], ["^[A-Z]+$", "^[a-z]+$"])
        self.assertEqual([r["classification"] for r in rows], ["RESOLVED_CONSTANT"] * 2)
        self.assertEqual({b["name"] for b in rows[0]["constant_resolution"]}, {"START", "END"})

    def test_runtime_and_ambiguous_constants_are_unresolved(self):
        for prefix in ('PATTERN = "x"\nPATTERN = "y"\n',
                       'if flag:\n    PATTERN = "x"\n',
                       'PATTERN = make_pattern()\n',
                       'PATTERN = "x"\ndef run(PATTERN):\n    pass\n'):
            with self.subTest(prefix=prefix):
                rows = inventory(IMPORT + prefix + 'F.rlike("name", F.lit(PATTERN))')
                self.assertEqual(rows[0]["classification"], "DYNAMIC_PATTERN")

    def test_other_regex_functions_have_their_own_classification(self):
        rows = inventory(IMPORT + 'F.regexp_extract("name", "x", 0)')
        self.assertEqual(rows[0]["classification"], "NON_RLIKE_REGEX")

    def test_invalid_call_shapes_are_unknown(self):
        for expression in ('F.rlike("name")', 'F.rlike(*args)',
                           'F.rlike("name", F.lit("a", "b"))',
                           'F.rlike("name", F.lit("a"), unknown=True)'):
            with self.subTest(expression=expression):
                rows = inventory(IMPORT + expression)
                self.assertEqual(rows[0]["classification"], "UNKNOWN")

    def test_rule_definitions_require_a_manifest_consumer_contract(self):
        text = '''from example.rules import RuleDefinition
RuleDefinition(field="name", check_type="has_pattern", value="^[a-z]+$")
RuleDefinition(field="name", check_type="is_equal", value="not a pattern")
'''
        self.assertEqual(inventory(text), [])
        rows = inventory(text, rule_constructors=["example.rules.RuleDefinition"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["classification"], "RESOLVED_CONSTANT")
        self.assertFalse(rows[0]["api_binding_verified"])
        self.assertEqual(rows[0]["resolution_kind"], "reviewed_rule_constructor")

    def test_template_holes_are_audited_without_default_argument_propagation(self):
        from probe_corpora import discover_pyspark
        rows, _ = discover_pyspark('''def query(pattern="^default$"):
    return f"x RLIKE '{pattern}' and y RLIKE 'fixed'"
''', include_templates=True)
        self.assertEqual([r["classification"] for r in rows], ["DYNAMIC_PATTERN", "UNKNOWN"])
        self.assertTrue(all("pattern_value" not in r for r in rows))


if __name__ == "__main__":
    unittest.main()

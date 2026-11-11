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

"""Boundary checks for the discovery inventory, not regex compatibility tests."""

import unittest

from probe_corpora import discover, discover_pyspark


class ProbeCorpusTests(unittest.TestCase):
    def test_comments_strings_and_identifiers_are_not_predicates(self):
        rows, _ = discover("""
            -- x rlike 'comment'
            /* outer /* y regexp 'nested' */ comment */
            SELECT 'x rlike ''string''', `rlike`, "regexp", 1;
            DESCRIBE FUNCTION rlike;
            DESCRIBE FUNCTION EXTENDED regexp;
        """)
        self.assertEqual(rows, [])

    def test_negated_infix_and_function_aliases(self):
        rows, _ = discover("""SELECT x NOT RLIKE '^a', x REGEXP '[ab]',
            regexp_like(concat(x, ','), 'b'), rlike(x, 'c'), regexp(x, 'd');""")
        self.assertEqual([r["literal_sql"] for r in rows],
                         ["'^a'", "'[ab]'", "'b'", "'c'", "'d'"])
        self.assertTrue(all(r["status"] == "direct_literal_candidate" for r in rows))

    def test_literal_spelling_is_preserved_without_unescaping(self):
        sql = r"""SELECT x rlike '\\d', x rlike r'\d', x rlike '[.]',
            x rlike 'it\'s', x rlike 'it''s', x rlike "^ab";"""
        rows, _ = discover(sql)
        self.assertEqual([r["literal_sql"] for r in rows],
                         [r"'\\d'", r"r'\d'", "'[.]'", r"'it\'s'", "'it''s'", '"^ab"'])

    def test_dynamic_and_composed_expressions_are_not_direct_literals(self):
        rows, _ = discover("""SELECT x rlike pattern, rlike(x, concat('a', 'b')),
            x rlike 'a' || suffix, x rlike ('b'), regexp_like(x, NULL);""")
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(r["status"] != "direct_literal_candidate" for r in rows))

    def test_templates_need_expansion(self):
        rows, _ = discover("SELECT x rlike '${regex}', x rlike '{{ regex }}';")
        self.assertEqual([r["status"] for r in rows], ["templated_literal"] * 2)

    def test_other_regex_functions_are_separate(self):
        rows, other = discover("SELECT regexp_replace(x, 'a', 'b'), regexp_extract(x, 'c', 1);")
        self.assertEqual(rows, [])
        self.assertEqual(dict(other), {"regexp_replace": 1, "regexp_extract": 1})

    def test_duplicate_occurrences_and_coordinates(self):
        rows, _ = discover("SELECT x RLIKE 'a',\n  x RLIKE 'a';")
        self.assertEqual(len(rows), 2)
        self.assertEqual(len({r["literal_sql"] for r in rows}), 1)
        self.assertEqual([(r["line"], r["column"]) for r in rows], [(1, 10), (2, 5)])

    def test_broken_input_is_reported(self):
        for sql in ("SELECT 'unterminated", "/* unterminated", "SELECT $$unterminated"):
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                discover(sql)

    def test_dollar_quoted_text_is_opaque(self):
        rows, _ = discover("SELECT $$x rlike 'not SQL'$$, x rlike 'a';")
        self.assertEqual([r["literal_sql"] for r in rows], ["'a'"])


class PythonProbeTests(unittest.TestCase):
    def test_constants_have_python_values_and_preserve_source(self):
        text = r'''a.rlike(r"\d+")
b.rlike("\\d+")
c.rlike("[.]" "[a-z]")'''
        rows, _ = discover_pyspark(text)
        self.assertEqual([r["pattern_value"] for r in rows],
                         [r"\d+", r"\d+", "[.][a-z]"])
        self.assertEqual([r["literal_python"] for r in rows],
                         ['r"\\d+"', '"\\\\d+"', '"[.]" "[a-z]"'])

    def test_comments_docstrings_and_embedded_sql_are_not_calls(self):
        text = '''# a.rlike("comment")
"""b.rlike("docstring")"""
spark.sql("SELECT x RLIKE 'embedded'")
c.rlike("actual")'''
        rows, _ = discover_pyspark(text)
        self.assertEqual([r["pattern_value"] for r in rows], ["actual"])

    def test_dynamic_expressions_are_visible_but_not_candidates(self):
        text = '''a.rlike(pattern)
a.rlike(f"^{name}$")
a.rlike("^" + name)
a.rlike(b"bytes")
a.rlike(other=variable)'''
        rows, _ = discover_pyspark(text)
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(r["status"] == "non_direct_literal_or_unparsed" for r in rows))

    def test_module_functions_have_different_string_semantics(self):
        text = '''import pyspark.sql.functions as F
from pyspark.sql import functions as f
from pyspark.sql.functions import rlike
import pyspark.sql.functions
F.rlike("col", "pattern_column")
f.rlike("col", F.lit("pattern"))
pyspark.sql.functions.rlike("col", "pattern_column")
rlike("col", "pattern_column")
F.col("col").rlike("actual")'''
        rows, _ = discover_pyspark(text)
        self.assertEqual([r["pattern_value"] for r in rows if "pattern_value" in r], ["pattern", "actual"])
        self.assertEqual([r["classification"] for r in rows],
                         ["DYNAMIC_PATTERN", "DIRECT_LITERAL", "DYNAMIC_PATTERN", "DYNAMIC_PATTERN", "DIRECT_LITERAL"])

    def test_coordinates_and_duplicates(self):
        rows, _ = discover_pyspark('é.rlike("a"); é.rlike("a")')
        self.assertEqual([(r["line"], r["column"]) for r in rows], [(1, 3), (1, 17)])
        self.assertEqual(len(rows), 2)
        self.assertEqual(len({r["pattern_value"] for r in rows}), 1)

    def test_source_is_parsed_without_execution(self):
        rows, _ = discover_pyspark('raise RuntimeError("do not execute")\na.rlike("x")')
        self.assertEqual(rows[0]["pattern_value"], "x")
        with self.assertRaises(SyntaxError):
            discover_pyspark('a.rlike("x"')


if __name__ == "__main__":
    unittest.main()

-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- translate runs through the codegen dispatcher by default so results match Spark exactly. The
-- native path diverges from Spark (DataFusion iterates over Unicode graphemes where Spark uses code
-- points, and substitutes U+0000 instead of treating it as a deletion sentinel), so it is opt-in
-- via spark.comet.expression.StringTranslate.allowIncompatible. See string_translate_enabled.sql
-- for the opt-in native path.

statement
CREATE TABLE test_translate(s string, from_str string, to_str string) USING parquet

-- The last two rows are the regression test for this file's routing change: they are the inputs
-- where the native path is known to disagree with Spark, so they are only assertable now that the
-- default is the bit-exact dispatcher. Written with \u escapes rather than literal characters to
-- keep the fixture ASCII. They have to be plain string literals: an inline table only accepts
-- expressions the analyzer can evaluate, and `decode(X'..')` is not one of them on Spark 4.1
-- (INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE).
--   'cafe\u0301' ends in "e" + U+0301 COMBINING ACUTE ACCENT: one grapheme, two code
--     points. DataFusion iterates graphemes, Spark iterates code points, so translating "e"
--     differs.
--   '\u0000' as the `to` argument is U+0000, which Spark treats as a deletion sentinel and
--     the native path substitutes it literally.
statement
INSERT INTO test_translate VALUES
  ('hello', 'el', 'ip'), ('hello', 'aeiou', '12345'), ('', 'a', 'b'), (NULL, 'a', 'b'),
  ('hello', '', ''), ('abc', 'abc', 'x'),
  ('cafe\u0301', 'e', 'E'),
  ('hello', 'l', '\u0000')

query
SELECT translate(s, from_str, to_str) FROM test_translate

-- column + literal + literal
query
SELECT translate(s, 'el', 'ip') FROM test_translate

-- literal + column + column
query
SELECT translate('hello', from_str, to_str) FROM test_translate

-- literal + literal + literal
query
SELECT translate('hello', 'el', 'ip'), translate('hello', 'aeiou', '12345'), translate('', 'a', 'b'), translate(NULL, 'a', 'b')

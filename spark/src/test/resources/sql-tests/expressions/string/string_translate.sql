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

-- translate is gated as Incompatible by default. DataFusion's translate iterates over Unicode
-- graphemes (Spark uses code points) and substitutes U+0000 instead of treating it as a deletion
-- sentinel, so the native path silently diverges from Spark for combining-mark inputs and for
-- to=NUL. These default-config tests assert that the expression falls back cleanly to Spark.
-- See string_translate_enabled.sql for the opt-in native path.

statement
CREATE TABLE test_translate(s string, from_str string, to_str string) USING parquet

statement
INSERT INTO test_translate VALUES ('hello', 'el', 'ip'), ('hello', 'aeiou', '12345'), ('', 'a', 'b'), (NULL, 'a', 'b'), ('hello', '', ''), ('abc', 'abc', 'x')

query expect_fallback(is not fully compatible with Spark)
SELECT translate(s, from_str, to_str) FROM test_translate

-- column + literal + literal
query expect_fallback(is not fully compatible with Spark)
SELECT translate(s, 'el', 'ip') FROM test_translate

-- literal + column + column
query expect_fallback(is not fully compatible with Spark)
SELECT translate('hello', from_str, to_str) FROM test_translate

-- literal + literal + literal
query expect_fallback(is not fully compatible with Spark)
SELECT translate('hello', 'el', 'ip'), translate('hello', 'aeiou', '12345'), translate('', 'a', 'b'), translate(NULL, 'a', 'b')

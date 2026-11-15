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

-- Test RLIKE dispatcher patterns and the native empty-pattern case
-- Raw literals keep Spark from dropping regex backslashes such as '\d'.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.expression.RLike.allowIncompatible=false

statement
CREATE TABLE test_rlike_java(s string) USING parquet

statement
INSERT INTO test_rlike_java VALUES
  ('hello'), ('12345'), (''), (NULL), ('Hello World'), ('abc123'), ('aa'), ('ab'),
  ('no digits'), ('mixed_42_data'), ('xyzzy'), ('foobar'), ('foobaz'), ('barfoo'),
  ('barbar'), ('foofoo'), ('FOO'), ('foo'), ('fOO'), ('bar'), ('a1'), ('9z'), ('abc'), ('x')

query expect_dispatch(rlike)
SELECT s RLIKE r'^\d+$' FROM test_rlike_java

query expect_dispatch(rlike)
SELECT s RLIKE '^[a-z]+$' FROM test_rlike_java

query expect_native(rlike)
SELECT s, s RLIKE '' FROM test_rlike_java

-- backreference (Java-only)
query expect_dispatch(rlike)
SELECT s, s RLIKE r'^(\w)\1$' FROM test_rlike_java

-- lookahead (Java-only)
query expect_dispatch(rlike)
SELECT s RLIKE r'abc(?=\d)' FROM test_rlike_java

-- embedded flags (Java-only)
query expect_dispatch(rlike)
SELECT s RLIKE '(?i)hello' FROM test_rlike_java

-- Literal arguments mix native (empty pattern) and dispatched RLIKE, so only check parity.
query
SELECT 'hello' RLIKE '^[a-z]+$', '12345' RLIKE r'^\d+$', '' RLIKE '', NULL RLIKE 'a'

-- Substring matches differ from the anchored digit pattern above.
query expect_dispatch(rlike)
SELECT s, s RLIKE r'\d+' AS m FROM test_rlike_java

query expect_dispatch(rlike)
SELECT s FROM test_rlike_java WHERE s RLIKE r'\d+'

query expect_dispatch(rlike)
SELECT s FROM test_rlike_java WHERE s RLIKE r'^(\w)\1$'

-- Preserve the original lookaround, flag and named-group patterns and subjects.
query expect_dispatch(rlike)
SELECT s, s RLIKE r'foo(?=bar)' FROM test_rlike_java

query expect_dispatch(rlike)
SELECT s FROM test_rlike_java WHERE s RLIKE r'foo(?=bar)'

query expect_dispatch(rlike)
SELECT s, s RLIKE r'(?<=foo)bar' FROM test_rlike_java

query expect_dispatch(rlike)
SELECT s, s RLIKE r'(?i)foo' FROM test_rlike_java

query expect_dispatch(rlike)
SELECT s, s RLIKE r'(?<digit>\d)' FROM test_rlike_java

-- Anchors use Java semantics, including the empty-subject case.
query expect_dispatch(rlike)
SELECT s, s RLIKE '^$' FROM test_rlike_java

statement
CREATE TABLE test_rlike_java_nulls(s string) USING parquet

statement
INSERT INTO test_rlike_java_nulls VALUES (NULL), (NULL), (NULL)

query expect_dispatch(rlike)
SELECT s RLIKE r'\d+' FROM test_rlike_java_nulls

statement
CREATE TABLE test_rlike_java_grouped(s string, k int) USING parquet

statement
INSERT INTO test_rlike_java_grouped VALUES ('aa', 1), ('ab', 1), ('aa', 2), ('xyzzy', 2), ('aa', 3), (NULL, 3)

query expect_dispatch(rlike)
SELECT k, COUNT(*) AS c FROM test_rlike_java_grouped WHERE s RLIKE r'^(\w)\1$' GROUP BY k ORDER BY k

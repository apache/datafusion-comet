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

-- MinSparkVersion: 4.0

-- Spark 4.0+ widens Levenshtein's arguments to collated strings, but Levenshtein computes the
-- distance with UTF8String.levenshteinDistance, which walks UTF-8 code points and takes no
-- collation. The result is an IntegerType, so collation cannot change the answer. The native
-- kernel compares raw bytes though, so a collated argument is reported as Unsupported and routed
-- through the JVM codegen dispatcher, which runs Spark's own doGenCode inside the Comet pipeline.
-- That keeps the projection in Comet and is measurably faster than the native path: 341ms against
-- 378ms for Spark over 1M rows. See https://github.com/apache/datafusion-comet/issues/5591.

-- This harness disables ConstantFolding, so `x COLLATE ...` survives as a `Collate` node rather
-- than folding into a collated literal. Comet has no serde for `Collate`, but that does not matter
-- here: the dispatcher compiles the whole bound subtree with Spark's own doGenCode, `Collate`
-- included. That is one reason dispatching covers every spelling of a collated argument.

statement
CREATE TABLE test_levenshtein_collated(s1 string, s2 string) USING parquet

statement
INSERT INTO test_levenshtein_collated VALUES ('kitten', 'sitting'), ('frog', 'fog'), ('HELLO', 'hello'), ('abc', 'abc'), ('', 'hello'), (NULL, 'test'), ('hello', NULL), (NULL, NULL)

-- both arguments collated, column form
query
SELECT s1, s2, levenshtein(s1 COLLATE UTF8_LCASE, s2 COLLATE UTF8_LCASE) FROM test_levenshtein_collated

-- The load-bearing case. Spark's levenshtein is NOT collation-aware: under UTF8_LCASE the two
-- values still differ in all five characters, so the answer is 5 and not 0. This is why the
-- byte-oriented native kernel already matches Spark.
query
SELECT levenshtein('HELLO' COLLATE UTF8_LCASE, 'hello' COLLATE UTF8_LCASE)

-- only one side collated; the other is implicitly cast to the collated type
query
SELECT levenshtein(s1 COLLATE UTF8_LCASE, s2) FROM test_levenshtein_collated

query
SELECT levenshtein(s1, s2 COLLATE UTF8_LCASE) FROM test_levenshtein_collated

-- three-argument form takes Levenshtein's other codegen branch (genCodeWithThreshold)
query
SELECT levenshtein(s1 COLLATE UTF8_LCASE, s2 COLLATE UTF8_LCASE, 2) FROM test_levenshtein_collated

query
SELECT levenshtein('kitten' COLLATE UTF8_LCASE, 'sitting' COLLATE UTF8_LCASE, 1)

-- a second ICU collation, to show the guard is not UTF8_LCASE-only
query
SELECT levenshtein(s1 COLLATE UNICODE_CI, s2 COLLATE UNICODE_CI) FROM test_levenshtein_collated

query
SELECT levenshtein('HELLO' COLLATE UNICODE_CI, 'hello' COLLATE UNICODE_CI)

-- non-ASCII input under a collation, mirroring the unicode case in levenshtein.sql
query
SELECT levenshtein('café' COLLATE UTF8_LCASE, 'cafe' COLLATE UTF8_LCASE), levenshtein('你好' COLLATE UTF8_LCASE, '你坏' COLLATE UTF8_LCASE)

-- RTRIM collations are the case where trimming looks plausible: Levenshtein's inputTypes accept
-- them (StringTypeWithCollation(supportsTrimCollation = true)), and CollationFactory right-trims
-- when building a *collation key*. UTF8String.levenshteinDistance never builds one, so the
-- trailing space still counts as an edit and the distance is 1, not 0.
query
SELECT levenshtein('abc ' COLLATE UTF8_BINARY_RTRIM, 'abc' COLLATE UTF8_BINARY_RTRIM)

query
SELECT levenshtein('ABC ' COLLATE UTF8_LCASE_RTRIM, 'abc' COLLATE UTF8_LCASE_RTRIM)

-- A NULL-literal argument is omitted: Levenshtein is nullIntolerant, so NullPropagation folds the
-- whole call to a NULL literal before Comet sees it. The NULL rows in the table above cover
-- NULL-in-data for both arguments.

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

-- Test regexp_extract_all() with the per-expression allowIncompatible flag enabled (happy path).
-- Config: spark.comet.expression.RegExpExtractAll.allowIncompatible=true

statement
CREATE TABLE test_regexp_extract_all_enabled(s string) USING parquet

statement
INSERT INTO test_regexp_extract_all_enabled VALUES
  ('100-200, 300-400'),
  ('foo-bar'),
  ('nodelim'),
  ('12-34-56'),
  (''),
  (NULL),
  ('phone 123-456-7890')

-- group 1 across every match
query
SELECT regexp_extract_all(s, '(\\d+)-(\\d+)', 1) FROM test_regexp_extract_all_enabled

-- group 2 across every match
query
SELECT regexp_extract_all(s, '(\\d+)-(\\d+)', 2) FROM test_regexp_extract_all_enabled

-- idx = 0 returns every entire match
query
SELECT regexp_extract_all(s, '(\\d+)-(\\d+)', 0) FROM test_regexp_extract_all_enabled

-- default idx (no third arg) is 1
query
SELECT regexp_extract_all(s, '(\\d+)-(\\d+)') FROM test_regexp_extract_all_enabled

-- single-group match; no match should produce empty array, NULL input -> NULL
query
SELECT regexp_extract_all(s, '(\\d+)', 1) FROM test_regexp_extract_all_enabled

-- optional unmatched group should contribute the empty string
query
SELECT regexp_extract_all(s, '(\\w+)( \\d+)?', 2) FROM test_regexp_extract_all_enabled

-- anchors and character classes
query
SELECT regexp_extract_all(s, '^(\\w+)', 1) FROM test_regexp_extract_all_enabled

query
SELECT regexp_extract_all(s, '(\\d+)$', 1) FROM test_regexp_extract_all_enabled

-- literal arguments
query
SELECT
  regexp_extract_all('alice@example.com, bob@example.org', '([\\w.+-]+)@([\\w.-]+)', 1),
  regexp_extract_all('alice@example.com, bob@example.org', '([\\w.+-]+)@([\\w.-]+)', 2),
  regexp_extract_all('not-an-email', '([\\w.+-]+)@([\\w.-]+)', 1),
  regexp_extract_all(NULL, '(\\d+)', 1)

-- NULL pattern propagates as NULL (Spark and Comet both return NULL)
query
SELECT regexp_extract_all(s, CAST(NULL AS STRING), 1) FROM test_regexp_extract_all_enabled

-- NULL idx propagates as NULL
query
SELECT regexp_extract_all(s, '(\\d+)-(\\d+)', CAST(NULL AS INT)) FROM test_regexp_extract_all_enabled

-- idx = 0 with no capture groups returns every whole match
query
SELECT regexp_extract_all(s, '\\d+', 0) FROM test_regexp_extract_all_enabled

-- multibyte / Unicode subject
statement
CREATE TABLE test_regexp_extract_all_unicode(s string) USING parquet

statement
INSERT INTO test_regexp_extract_all_unicode VALUES
  ('café=42, hot=99'),
  ('世界=1, 東京=2'),
  ('🔥=hot, ❄=cold'),
  ('मानक=हिन्दी')

-- ASCII anchors and capture groups against multibyte data
query
SELECT regexp_extract_all(s, '(\\S+)=(\\S+)', 1) FROM test_regexp_extract_all_unicode

query
SELECT regexp_extract_all(s, '(\\S+)=(\\S+)', 2) FROM test_regexp_extract_all_unicode

-- digit class against multibyte data
query
SELECT regexp_extract_all(s, '=(\\d+)', 1) FROM test_regexp_extract_all_unicode

-- ERROR CASES
-- idx > groupCount (pattern has 2 groups, ask for 3)
query expect_error(group index)
SELECT regexp_extract_all(s, '(\\d+)-(\\d+)', 3) FROM test_regexp_extract_all_enabled

-- pattern with no capture groups but idx >= 1
query expect_error(group index)
SELECT regexp_extract_all(s, '\\d+', 1) FROM test_regexp_extract_all_enabled

-- negative idx
query expect_error(group index)
SELECT regexp_extract_all(s, '(\\d+)-(\\d+)', -1) FROM test_regexp_extract_all_enabled

-- invalid regex syntax (unclosed group): both engines fail at pattern compile time.
-- Spark surfaces INVALID_PARAMETER_VALUE.PATTERN, Comet surfaces a regex parse error.
-- Both messages mention `regexp_extract_all`.
query expect_error(regexp_extract_all)
SELECT regexp_extract_all(s, '(unclosed', 1) FROM test_regexp_extract_all_enabled

-- Java-only regex feature: lookahead. Rust regex rejects this at compile time;
-- Spark accepts it. This is one of the documented incompatibilities behind the
-- Incompatible support level, not an invariant we test for cross-engine equivalence.
query ignore(Rust regex does not support lookahead, unlike Java regex)
SELECT regexp_extract_all(s, '(?=\\d)\\w+', 0) FROM test_regexp_extract_all_enabled

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

-- Test regexp_extract() with the per-expression allowIncompatible flag enabled (happy path).
-- Config: spark.comet.expression.RegExpExtract.allowIncompatible=true

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_regexp_extract_enabled(s string) USING parquet

statement
INSERT INTO test_regexp_extract_enabled VALUES
  ('100-200'),
  ('foo-bar'),
  ('nodelim'),
  ('12-34-56'),
  (''),
  (NULL),
  ('phone 123-456-7890')

-- group 1 of the first match
query
SELECT regexp_extract(s, '(\\d+)-(\\d+)', 1) FROM test_regexp_extract_enabled

-- group 2 of the first match
query
SELECT regexp_extract(s, '(\\d+)-(\\d+)', 2) FROM test_regexp_extract_enabled

-- idx = 0 returns the entire match
query
SELECT regexp_extract(s, '(\\d+)-(\\d+)', 0) FROM test_regexp_extract_enabled

-- default idx (no third arg) is 1
query
SELECT regexp_extract(s, '(\\d+)-(\\d+)') FROM test_regexp_extract_enabled

-- single-group match; no match should produce empty string, NULL input -> NULL
query
SELECT regexp_extract(s, '(\\d+)', 1) FROM test_regexp_extract_enabled

-- optional unmatched group should return empty string
query
SELECT regexp_extract(s, '(\\w+)( \\d+)?', 2) FROM test_regexp_extract_enabled

-- anchors and character classes
query
SELECT regexp_extract(s, '^(\\w+)', 1) FROM test_regexp_extract_enabled

query
SELECT regexp_extract(s, '(\\d+)$', 1) FROM test_regexp_extract_enabled

-- literal arguments
query
SELECT
  regexp_extract('alice@example.com', '^([\\w.+-]+)@([\\w.-]+)$', 1),
  regexp_extract('alice@example.com', '^([\\w.+-]+)@([\\w.-]+)$', 2),
  regexp_extract('not-an-email', '^([\\w.+-]+)@([\\w.-]+)$', 1),
  regexp_extract(NULL, '(\\d+)', 1)

-- NULL pattern propagates as NULL (Spark and Comet both return NULL)
query
SELECT regexp_extract(s, CAST(NULL AS STRING), 1) FROM test_regexp_extract_enabled

-- NULL idx propagates as NULL
query
SELECT regexp_extract(s, '(\\d+)-(\\d+)', CAST(NULL AS INT)) FROM test_regexp_extract_enabled

-- idx = 0 with no capture groups returns the whole match
query
SELECT regexp_extract(s, '\\d+', 0) FROM test_regexp_extract_enabled

-- multibyte / Unicode subject
statement
CREATE TABLE test_regexp_extract_unicode(s string) USING parquet

statement
INSERT INTO test_regexp_extract_unicode VALUES
  ('café=42'),
  ('café=99'),
  ('世界=1'),
  ('日本=東京'),
  ('🔥=hot'),
  ('मानक=हिन्दी')

-- ASCII anchors and capture groups against multibyte data
query
SELECT regexp_extract(s, '^(.+)=(.+)$', 1) FROM test_regexp_extract_unicode

query
SELECT regexp_extract(s, '^(.+)=(.+)$', 2) FROM test_regexp_extract_unicode

-- digit class against multibyte data
query
SELECT regexp_extract(s, '=(\\d+)$', 1) FROM test_regexp_extract_unicode

-- ERROR CASES
-- idx > groupCount (pattern has 2 groups, ask for 3)
query expect_error(group index)
SELECT regexp_extract(s, '(\\d+)-(\\d+)', 3) FROM test_regexp_extract_enabled

-- pattern with no capture groups but idx >= 1
query expect_error(group index)
SELECT regexp_extract(s, '\\d+', 1) FROM test_regexp_extract_enabled

-- negative idx
query expect_error(group index)
SELECT regexp_extract(s, '(\\d+)-(\\d+)', -1) FROM test_regexp_extract_enabled

-- invalid regex syntax (unclosed group): both engines fail at pattern compile time.
-- Spark surfaces INVALID_PARAMETER_VALUE.PATTERN, Comet surfaces a regex parse error.
-- Both messages mention `regexp_extract`.
query expect_error(regexp_extract)
SELECT regexp_extract(s, '(unclosed', 1) FROM test_regexp_extract_enabled

-- Java-only regex feature: lookahead. Rust regex rejects this at compile time;
-- Spark accepts it and returns "" for every row. This is one of the documented
-- incompatibilities behind the Incompatible support level, not an invariant we
-- test for cross-engine equivalence.
query ignore(Rust regex does not support lookahead, unlike Java regex)
SELECT regexp_extract(s, '(?=\\d)\\w+', 0) FROM test_regexp_extract_enabled

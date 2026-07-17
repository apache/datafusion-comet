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

-- any_value is a RuntimeReplaceableAggregate that Spark's optimizer rewrites
-- to first(expr, ignoreNulls) before physical planning, so these tests exercise
-- the same Comet path as first_last.sql but pin the any_value name explicitly.

-- ============================================================
-- Setup: shared tables
-- ============================================================

statement
CREATE TABLE test_any_value(i int, grp string) USING parquet

statement
INSERT INTO test_any_value VALUES (1, 'a'), (2, 'a'), (3, 'a'), (NULL, 'b'), (4, 'b')

statement
CREATE TABLE test_any_value_ignore_nulls(id int, val int, grp string) USING parquet

statement
INSERT INTO test_any_value_ignore_nulls VALUES
  (1, NULL, 'a'),
  (2, 10,   'a'),
  (3, 20,   'a'),
  (4, NULL, 'b'),
  (5, 30,   'b'),
  (6, NULL, 'b')

statement
CREATE TABLE test_any_value_all_nulls(val int, grp string) USING parquet

statement
INSERT INTO test_any_value_all_nulls VALUES (NULL, 'a'), (NULL, 'a'), (NULL, 'b'), (1, 'b')

-- ============================================================
-- any_value: basic (default behavior includes nulls)
-- ============================================================

query
SELECT any_value(i) FROM test_any_value

query
SELECT grp, any_value(i) FROM test_any_value GROUP BY grp ORDER BY grp

-- ============================================================
-- any_value with isIgnoreNull literal
-- ============================================================

-- any_value(expr, true) ignores nulls.
query
SELECT any_value(val, true) FROM test_any_value_ignore_nulls

query
SELECT grp, any_value(val, true) FROM test_any_value_ignore_nulls GROUP BY grp ORDER BY grp

-- any_value(expr, false) respects nulls (default).
query
SELECT grp, any_value(val, false) FROM test_any_value_ignore_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- any_value: all-null group with ignoreNulls
-- ============================================================

-- Group 'a' has all nulls so result is NULL even when ignoring nulls.
query
SELECT grp, any_value(val, true) FROM test_any_value_all_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- any_value alongside other aggregates
-- ============================================================

query
SELECT grp,
  any_value(val, true),
  count(val),
  sum(val)
FROM test_any_value_ignore_nulls GROUP BY grp ORDER BY grp

-- ============================================================
-- any_value matches first when called identically
-- ============================================================

-- Same input expression and same ignoreNulls flag should yield identical results
-- because Spark rewrites any_value(x, b) to first(x, b).
query
SELECT
  any_value(val, true) = first(val, true) AS ignore_nulls_match,
  any_value(val, false) = first(val, false) AS respect_nulls_match
FROM test_any_value_ignore_nulls

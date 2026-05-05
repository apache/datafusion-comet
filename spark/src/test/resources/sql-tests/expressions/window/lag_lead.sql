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

-- Config: spark.comet.operator.WindowExec.allowIncompatible=true

-- ============================================================
-- Setup: shared tables
-- ============================================================

statement
CREATE TABLE test_lag_lead(id int, val int, grp string) USING parquet

statement
INSERT INTO test_lag_lead VALUES
  (1, 10, 'a'),
  (2, 20, 'a'),
  (3, 30, 'a'),
  (4, 40, 'b'),
  (5, 50, 'b')

statement
CREATE TABLE test_nulls(id int, val int, grp string) USING parquet

statement
INSERT INTO test_nulls VALUES
  (1, NULL, 'a'),
  (2, 10,   'a'),
  (3, NULL, 'a'),
  (4, 20,   'a'),
  (5, NULL, 'b'),
  (6, 30,   'b'),
  (7, NULL, 'b')

statement
CREATE TABLE test_all_nulls(id int, val int, grp string) USING parquet

statement
INSERT INTO test_all_nulls VALUES
  (1, NULL, 'a'),
  (2, NULL, 'a'),
  (3, NULL, 'b'),
  (4, 1,    'b')

statement
CREATE TABLE test_single_row(id int, val int) USING parquet

statement
INSERT INTO test_single_row VALUES (1, 42)

statement
CREATE TABLE test_types(
  id int,
  i_val int,
  l_val bigint,
  d_val double,
  s_val string,
  grp string
) USING parquet

statement
INSERT INTO test_types VALUES
  (1, NULL, NULL,  NULL,  NULL,  'a'),
  (2, 1,    100,   1.5,   'foo', 'a'),
  (3, 2,    200,   2.5,   'bar', 'a'),
  (4, NULL, NULL,  NULL,  NULL,  'b'),
  (5, 3,    300,   3.5,   'baz', 'b')

-- ############################################################
-- LAG
-- ############################################################

-- ============================================================
-- lag: basic (default offset = 1)
-- ============================================================

query
SELECT id, val,
  LAG(val) OVER (ORDER BY id) as lag_val
FROM test_lag_lead

query
SELECT grp, id, val,
  LAG(val) OVER (PARTITION BY grp ORDER BY id) as lag_val
FROM test_lag_lead

-- ============================================================
-- lag: with explicit offset
-- ============================================================

query
SELECT id, val,
  LAG(val, 2) OVER (ORDER BY id) as lag_val_2
FROM test_lag_lead

-- ============================================================
-- lag: with offset and default value
-- ============================================================

query
SELECT id, val,
  LAG(val, 2, -1) OVER (ORDER BY id) as lag_val_2
FROM test_lag_lead

-- ============================================================
-- lag IGNORE NULLS: basic
-- ============================================================

query
SELECT id, val,
  LAG(val) IGNORE NULLS OVER (ORDER BY id) as lag_val
FROM test_nulls

query
SELECT grp, id, val,
  LAG(val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) as lag_val
FROM test_nulls

-- ============================================================
-- lag IGNORE NULLS: all values null in a group
-- ============================================================

query
SELECT grp, id, val,
  LAG(val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) as lag_val
FROM test_all_nulls

-- ============================================================
-- lag IGNORE NULLS: single row
-- ============================================================

query
SELECT id, val,
  LAG(val) IGNORE NULLS OVER (ORDER BY id) as lag_val
FROM test_single_row

-- ============================================================
-- lag IGNORE NULLS: multiple data types
-- ============================================================

query
SELECT grp, id,
  LAG(i_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id),
  LAG(l_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id),
  LAG(d_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id),
  LAG(s_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id)
FROM test_types

-- ============================================================
-- lag IGNORE NULLS: with offset > 1
-- ============================================================

query
SELECT id, val,
  LAG(val, 2) IGNORE NULLS OVER (ORDER BY id) as lag_val_2
FROM test_nulls

-- ============================================================
-- lag: contrast IGNORE NULLS vs RESPECT NULLS
-- ============================================================

query
SELECT id, val,
  LAG(val) OVER (ORDER BY id) as lag_respect,
  LAG(val) IGNORE NULLS OVER (ORDER BY id) as lag_ignore
FROM test_nulls

-- ############################################################
-- LEAD
-- ############################################################

-- ============================================================
-- lead: basic (default offset = 1)
-- ============================================================

query
SELECT id, val,
  LEAD(val) OVER (ORDER BY id) as lead_val
FROM test_lag_lead

query
SELECT grp, id, val,
  LEAD(val) OVER (PARTITION BY grp ORDER BY id) as lead_val
FROM test_lag_lead

-- ============================================================
-- lead: with explicit offset
-- ============================================================

query
SELECT id, val,
  LEAD(val, 2) OVER (ORDER BY id) as lead_val_2
FROM test_lag_lead

-- ============================================================
-- lead: with offset and default value
-- ============================================================

query
SELECT id, val,
  LEAD(val, 2, -1) OVER (ORDER BY id) as lead_val_2
FROM test_lag_lead

-- ============================================================
-- lead IGNORE NULLS: basic
-- ============================================================

query
SELECT id, val,
  LEAD(val) IGNORE NULLS OVER (ORDER BY id) as lead_val
FROM test_nulls

query
SELECT grp, id, val,
  LEAD(val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) as lead_val
FROM test_nulls

-- ============================================================
-- lead IGNORE NULLS: all values null in a group
-- ============================================================

query
SELECT grp, id, val,
  LEAD(val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) as lead_val
FROM test_all_nulls

-- ============================================================
-- lead IGNORE NULLS: single row
-- ============================================================

query
SELECT id, val,
  LEAD(val) IGNORE NULLS OVER (ORDER BY id) as lead_val
FROM test_single_row

-- ============================================================
-- lead IGNORE NULLS: multiple data types
-- ============================================================

query
SELECT grp, id,
  LEAD(i_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id),
  LEAD(l_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id),
  LEAD(d_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id),
  LEAD(s_val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id)
FROM test_types

-- ============================================================
-- lead IGNORE NULLS: with offset > 1
-- ============================================================

query
SELECT id, val,
  LEAD(val, 2) IGNORE NULLS OVER (ORDER BY id) as lead_val_2
FROM test_nulls

-- ============================================================
-- lead: contrast IGNORE NULLS vs RESPECT NULLS
-- ============================================================

query
SELECT id, val,
  LEAD(val) OVER (ORDER BY id) as lead_respect,
  LEAD(val) IGNORE NULLS OVER (ORDER BY id) as lead_ignore
FROM test_nulls

-- ############################################################
-- LAG + LEAD combined
-- ############################################################

query
SELECT id, val,
  LAG(val) OVER (ORDER BY id) as lag_val,
  LEAD(val) OVER (ORDER BY id) as lead_val
FROM test_lag_lead

query
SELECT grp, id, val,
  LAG(val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) as lag_ignore,
  LEAD(val) IGNORE NULLS OVER (PARTITION BY grp ORDER BY id) as lead_ignore
FROM test_nulls

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

-- Config: spark.comet.expression.CollectSet.allowIncompatible=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- ============================================================
-- Setup: tables
-- ============================================================

statement
CREATE TABLE test_collect_set_int(i int, grp string) USING parquet

statement
INSERT INTO test_collect_set_int VALUES
  (1, 'a'), (2, 'a'), (1, 'a'), (3, 'a'),
  (4, 'b'), (4, 'b'), (NULL, 'b'), (5, 'b')

statement
CREATE TABLE test_collect_set_types(
  b boolean, bi bigint, d double, s string, dc decimal(10,2), dt date, grp string
) USING parquet

statement
INSERT INTO test_collect_set_types VALUES
  (true,  10, 1.1, 'x', 1.50, DATE '2024-01-01', 'a'),
  (false, 20, 2.2, 'y', 2.50, DATE '2024-01-02', 'a'),
  (true,  10, 1.1, 'x', 1.50, DATE '2024-01-01', 'a'),
  (NULL,  30, 3.3, 'z', 3.50, DATE '2024-01-03', 'b'),
  (true,  30, 3.3, 'z', 3.50, DATE '2024-01-03', 'b')

statement
CREATE TABLE test_collect_set_nulls(val int, grp string) USING parquet

statement
INSERT INTO test_collect_set_nulls VALUES
  (NULL, 'a'), (NULL, 'a'), (NULL, 'b'), (1, 'b')

statement
CREATE TABLE test_collect_set_empty(val int) USING parquet

statement
CREATE TABLE test_collect_set_single(val int) USING parquet

statement
INSERT INTO test_collect_set_single VALUES (42)

-- ============================================================
-- Note: collect_set result ordering is non-deterministic.
-- We materialize aggregate results via CTAS and then sort
-- the arrays in a separate query to avoid sort_array in the
-- aggregate result expressions (which would cause the Final
-- aggregate to fall back to Spark).
-- ============================================================

-- ============================================================
-- Operator coverage: verify collect_set runs natively
-- (use size() which is supported, avoids array ordering issues)
-- ============================================================

query
SELECT grp, size(collect_set(i)) FROM test_collect_set_int GROUP BY grp ORDER BY grp

-- ============================================================
-- Basic: integer dedup
-- ============================================================

statement
CREATE TABLE cs_basic USING parquet AS
SELECT collect_set(i) as cs FROM test_collect_set_int

query spark_answer_only
SELECT sort_array(cs) FROM cs_basic

-- ============================================================
-- GROUP BY: integer dedup per group
-- ============================================================

statement
CREATE TABLE cs_grp_int USING parquet AS
SELECT grp, collect_set(i) as cs FROM test_collect_set_int GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_grp_int ORDER BY grp

-- ============================================================
-- NULLs: all NULLs in a group returns empty array
-- ============================================================

statement
CREATE TABLE cs_nulls USING parquet AS
SELECT grp, collect_set(val) as cs FROM test_collect_set_nulls GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_nulls ORDER BY grp

-- ============================================================
-- Empty table: returns empty array
-- ============================================================

statement
CREATE TABLE cs_empty USING parquet AS
SELECT collect_set(val) as cs FROM test_collect_set_empty

query spark_answer_only
SELECT sort_array(cs) FROM cs_empty

-- ============================================================
-- Single value
-- ============================================================

statement
CREATE TABLE cs_single USING parquet AS
SELECT collect_set(val) as cs FROM test_collect_set_single

query spark_answer_only
SELECT sort_array(cs) FROM cs_single

-- ============================================================
-- Multiple data types
-- ============================================================

-- boolean
statement
CREATE TABLE cs_bool USING parquet AS
SELECT grp, collect_set(b) as cs FROM test_collect_set_types GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_bool ORDER BY grp

-- bigint
statement
CREATE TABLE cs_bigint USING parquet AS
SELECT grp, collect_set(bi) as cs FROM test_collect_set_types GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_bigint ORDER BY grp

-- double
statement
CREATE TABLE cs_double USING parquet AS
SELECT grp, collect_set(d) as cs FROM test_collect_set_types GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_double ORDER BY grp

-- string
statement
CREATE TABLE cs_string USING parquet AS
SELECT grp, collect_set(s) as cs FROM test_collect_set_types GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_string ORDER BY grp

-- decimal
statement
CREATE TABLE cs_decimal USING parquet AS
SELECT grp, collect_set(dc) as cs FROM test_collect_set_types GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_decimal ORDER BY grp

-- date
statement
CREATE TABLE cs_date USING parquet AS
SELECT grp, collect_set(dt) as cs FROM test_collect_set_types GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_date ORDER BY grp

-- ============================================================
-- Mixed with other aggregates
-- ============================================================

statement
CREATE TABLE cs_mixed USING parquet AS
SELECT grp, collect_set(i) as cs, count(*) as cnt, sum(i) as total
FROM test_collect_set_int GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs), cnt, total FROM cs_mixed ORDER BY grp

-- ============================================================
-- All duplicates in a group
-- ============================================================

statement
CREATE TABLE test_collect_set_dupes(val int, grp string) USING parquet

statement
INSERT INTO test_collect_set_dupes VALUES (7, 'a'), (7, 'a'), (7, 'a'), (8, 'b'), (9, 'b')

statement
CREATE TABLE cs_dupes USING parquet AS
SELECT grp, collect_set(val) as cs FROM test_collect_set_dupes GROUP BY grp

query spark_answer_only
SELECT grp, sort_array(cs) FROM cs_dupes ORDER BY grp

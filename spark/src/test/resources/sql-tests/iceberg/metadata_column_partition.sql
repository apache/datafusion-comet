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

-- Config: spark.sql.catalog.test_cat=org.apache.iceberg.spark.SparkCatalog
-- Config: spark.sql.catalog.test_cat.type=hadoop
-- Config: spark.sql.catalog.test_cat.warehouse=/tmp/comet-iceberg-sql-test
-- Config: spark.comet.enabled=true
-- Config: spark.comet.exec.enabled=true
-- Config: spark.comet.iceberg.native.enabled=true

-- All `query` assertions below implicitly validate that Comet's partition type
-- computation (from partition_type_pool JSON) matches Spark's (from Iceberg Java's
-- PartitionSpec.partitionType()). If the two diverged, checkSparkAnswerAndOperator
-- would detect differing struct values. This covers the "dual computation path"
-- cross-validation concern.

-- =============================================================
-- Setup: Create table with bucket partitioning (format v2 for MoR)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.meta_test

statement
CREATE TABLE test_cat.db.meta_test (id INT, name STRING, value DOUBLE) USING iceberg PARTITIONED BY (bucket(4, id)) TBLPROPERTIES ('format-version' = '2')

statement
INSERT INTO test_cat.db.meta_test VALUES (1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'charlie', 30.0)

statement
INSERT INTO test_cat.db.meta_test VALUES (4, 'dave', 40.0), (5, 'eve', 50.0)

-- =============================================================
-- _file: basic projection (native)
-- =============================================================

query
SELECT id, _file FROM test_cat.db.meta_test ORDER BY id

-- Multiple inserts produce multiple files
query
SELECT COUNT(DISTINCT _file) > 1 FROM test_cat.db.meta_test

-- _file in a WHERE clause exercises filter pushdown interaction
query
SELECT id, name FROM test_cat.db.meta_test WHERE _file LIKE '%.parquet' ORDER BY id

-- =============================================================
-- _partition: basic struct projection (native)
-- =============================================================

query
SELECT id, _partition FROM test_cat.db.meta_test ORDER BY id

-- Access individual partition fields within the struct
query
SELECT id, _partition.id_bucket FROM test_cat.db.meta_test ORDER BY id

-- =============================================================
-- _spec_id: partition spec ID (scalar per file)
-- =============================================================

query
SELECT id, _spec_id FROM test_cat.db.meta_test ORDER BY id

-- All rows should have spec_id = 0 (initial spec)
query
SELECT DISTINCT _spec_id FROM test_cat.db.meta_test

-- =============================================================
-- _pos: row position within each file (0-based)
-- =============================================================

-- Basic: positions should be 0-based within each file
query
SELECT id, _file, _pos FROM test_cat.db.meta_test ORDER BY _file, _pos

-- Positions should be contiguous 0..N-1 per file
query
SELECT _file, MIN(_pos) as min_pos, MAX(_pos) as max_pos, COUNT(*) as cnt FROM test_cat.db.meta_test GROUP BY _file

-- =============================================================
-- Partition evolution requires IcebergSparkSessionExtensions
-- (ALTER TABLE ADD PARTITION FIELD / CALL system.add_partition_field).
-- The SQL file test framework cannot register session extensions via
-- Config directives. Partition evolution is covered in
-- CometIcebergNativeSuite which has full session control.
-- =============================================================

-- =============================================================
-- Combined: all metadata columns together (native -- all supported)
-- =============================================================

query
SELECT id, _file, _pos, _spec_id, _partition FROM test_cat.db.meta_test ORDER BY _file, _pos

-- Row-level operation simulation: what CoW/MoR would project
query
SELECT _file, _pos, _spec_id, _partition, id, name FROM test_cat.db.meta_test WHERE id > 3 ORDER BY _file, _pos

-- =============================================================
-- _file + _partition combined (native -- both supported)
-- =============================================================

query
SELECT id, _file, _partition FROM test_cat.db.meta_test ORDER BY id

statement
DROP TABLE test_cat.db.meta_test

-- =============================================================
-- Identity partition transform
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.identity_part

statement
CREATE TABLE test_cat.db.identity_part (id INT, name STRING, dept STRING) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.identity_part VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'sales'), (3, 'Charlie', 'eng')

query
SELECT id, _partition FROM test_cat.db.identity_part ORDER BY id

query
SELECT id, _partition.dept FROM test_cat.db.identity_part ORDER BY id

query
SELECT id, _file, _partition FROM test_cat.db.identity_part ORDER BY id

statement
DROP TABLE test_cat.db.identity_part

-- =============================================================
-- Multiple partition fields
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.multi_part

statement
CREATE TABLE test_cat.db.multi_part (id INT, year INT, month INT, data STRING) USING iceberg PARTITIONED BY (year, month)

statement
INSERT INTO test_cat.db.multi_part VALUES (1, 2023, 6, 'a'), (2, 2023, 7, 'b'), (3, 2024, 1, 'c')

query
SELECT id, _partition FROM test_cat.db.multi_part ORDER BY id

statement
DROP TABLE test_cat.db.multi_part

-- =============================================================
-- Partition with filter
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.filter_part

statement
CREATE TABLE test_cat.db.filter_part (id INT, dept STRING, value DOUBLE) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.filter_part VALUES (1, 'eng', 10.5), (2, 'sales', 20.3), (3, 'eng', 30.7)

query
SELECT id, _partition FROM test_cat.db.filter_part WHERE dept = 'eng' ORDER BY id

statement
DROP TABLE test_cat.db.filter_part

-- =============================================================
-- Edge case: Unpartitioned table
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.unpart

statement
CREATE TABLE test_cat.db.unpart (id INT, val STRING) USING iceberg

statement
INSERT INTO test_cat.db.unpart VALUES (1, 'a'), (2, 'b')

-- _partition on unpartitioned table is an empty struct (null), which Comet's
-- shuffle doesn't support. Verify correctness only, not native operator coverage.
query spark_answer_only
SELECT id, _partition FROM test_cat.db.unpart ORDER BY id

-- Without ORDER BY (no shuffle), the native scan executes end-to-end
query
SELECT id, _partition FROM test_cat.db.unpart

query
SELECT id, _file FROM test_cat.db.unpart ORDER BY id

-- _spec_id on unpartitioned: should be 0
-- (spark_answer_only because _partition is an empty struct that Comet shuffle rejects)
query spark_answer_only
SELECT id, _spec_id, _partition FROM test_cat.db.unpart ORDER BY id

statement
DROP TABLE test_cat.db.unpart

-- =============================================================
-- Edge case: Large batch (tests multi-batch _pos correctness)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.large_pos

statement
CREATE TABLE test_cat.db.large_pos (id INT) USING iceberg

statement
INSERT INTO test_cat.db.large_pos SELECT id FROM range(10000)

query
SELECT id, _pos FROM test_cat.db.large_pos ORDER BY id LIMIT 5

statement
DROP TABLE test_cat.db.large_pos

-- =============================================================
-- Edge case: Multiple row groups (forces _pos offset tracking)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.multi_rg

statement
CREATE TABLE test_cat.db.multi_rg (id INT, data STRING) USING iceberg TBLPROPERTIES ('write.parquet.row-group-size-bytes' = '1024')

statement
INSERT INTO test_cat.db.multi_rg SELECT id, REPEAT('x', 100) FROM range(1000)

query
SELECT _pos, id FROM test_cat.db.multi_rg WHERE _pos IN (0, 1, 998, 999) ORDER BY _pos

statement
DROP TABLE test_cat.db.multi_rg

-- =============================================================
-- DML operations (DELETE, UPDATE, MERGE) internally project metadata columns
-- (_file, _pos, _spec_id, _partition) to identify rows. All four are now
-- supported natively. The subsequent SELECT queries verify that reads after
-- DML return correct results through the native scan path (applying position
-- deletes, seeing updated data).
-- =============================================================

-- Copy-on-write DELETE
statement
DROP TABLE IF EXISTS test_cat.db.cow_delete

statement
CREATE TABLE test_cat.db.cow_delete (id INT, name STRING, dept STRING) USING iceberg PARTITIONED BY (dept) TBLPROPERTIES ('write.delete.mode'='copy-on-write')

statement
INSERT INTO test_cat.db.cow_delete VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'sales'), (3, 'Charlie', 'eng')

statement
DELETE FROM test_cat.db.cow_delete WHERE id = 2

query
SELECT id, _partition FROM test_cat.db.cow_delete ORDER BY id

statement
DROP TABLE test_cat.db.cow_delete

-- Merge-on-read DELETE (native read applies position deletes)
statement
DROP TABLE IF EXISTS test_cat.db.mor_delete

statement
CREATE TABLE test_cat.db.mor_delete (id INT, name STRING, dept STRING) USING iceberg PARTITIONED BY (dept) TBLPROPERTIES ('write.delete.mode'='merge-on-read')

statement
INSERT INTO test_cat.db.mor_delete VALUES (1, 'Alice', 'eng'), (2, 'Bob', 'sales'), (3, 'Charlie', 'eng')

statement
DELETE FROM test_cat.db.mor_delete WHERE id = 2

query
SELECT id, _partition FROM test_cat.db.mor_delete ORDER BY id

-- After MoR delete, _pos still references original file positions
query
SELECT id, _pos FROM test_cat.db.mor_delete ORDER BY id

statement
DROP TABLE test_cat.db.mor_delete

-- NOTE: Merge-on-read UPDATE is not tested here because UPDATE TABLE is not
-- supported in Spark 4.0. Covered in CometIcebergNativeSuite instead.

-- =============================================================
-- Temporal transforms: years() on DATE column
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_years_date

statement
CREATE TABLE test_cat.db.part_years_date (id INT, event_date DATE, data STRING) USING iceberg PARTITIONED BY (years(event_date))

statement
INSERT INTO test_cat.db.part_years_date VALUES (1, DATE '2022-03-15', 'a'), (2, DATE '2023-07-20', 'b'), (3, DATE '2022-11-01', 'c')

query
SELECT id, _partition FROM test_cat.db.part_years_date ORDER BY id

query
SELECT id, _partition.event_date_year FROM test_cat.db.part_years_date ORDER BY id

statement
DROP TABLE test_cat.db.part_years_date

-- =============================================================
-- Temporal transforms: months() on DATE column
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_months_date

statement
CREATE TABLE test_cat.db.part_months_date (id INT, event_date DATE, data STRING) USING iceberg PARTITIONED BY (months(event_date))

statement
INSERT INTO test_cat.db.part_months_date VALUES (1, DATE '2023-01-10', 'jan'), (2, DATE '2023-02-15', 'feb'), (3, DATE '2023-01-25', 'jan2')

query
SELECT id, _partition FROM test_cat.db.part_months_date ORDER BY id

query
SELECT id, _partition.event_date_month FROM test_cat.db.part_months_date ORDER BY id

statement
DROP TABLE test_cat.db.part_months_date

-- =============================================================
-- Temporal transforms: days() on DATE column
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_days_date

statement
CREATE TABLE test_cat.db.part_days_date (id INT, event_date DATE, data STRING) USING iceberg PARTITIONED BY (days(event_date))

statement
INSERT INTO test_cat.db.part_days_date VALUES (1, DATE '2023-06-01', 'x'), (2, DATE '2023-06-02', 'y'), (3, DATE '2023-06-01', 'z')

query
SELECT id, _partition FROM test_cat.db.part_days_date ORDER BY id

query
SELECT id, _partition.event_date_day FROM test_cat.db.part_days_date ORDER BY id

statement
DROP TABLE test_cat.db.part_days_date

-- =============================================================
-- Temporal transforms: hours() on TIMESTAMP column
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_hours_ts

statement
CREATE TABLE test_cat.db.part_hours_ts (id INT, ts TIMESTAMP, data STRING) USING iceberg PARTITIONED BY (hours(ts))

statement
INSERT INTO test_cat.db.part_hours_ts VALUES (1, TIMESTAMP '2023-06-15 10:30:00', 'morning'), (2, TIMESTAMP '2023-06-15 14:45:00', 'afternoon'), (3, TIMESTAMP '2023-06-15 10:55:00', 'morning2')

query
SELECT id, _partition FROM test_cat.db.part_hours_ts ORDER BY id

query
SELECT id, _partition.ts_hour FROM test_cat.db.part_hours_ts ORDER BY id

statement
DROP TABLE test_cat.db.part_hours_ts

-- =============================================================
-- Truncate transform: truncate(N, int_col)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_trunc_int

statement
CREATE TABLE test_cat.db.part_trunc_int (id INT, value INT, data STRING) USING iceberg PARTITIONED BY (truncate(10, value))

statement
INSERT INTO test_cat.db.part_trunc_int VALUES (1, 5, 'a'), (2, 15, 'b'), (3, 7, 'c'), (4, 23, 'd')

query
SELECT id, _partition FROM test_cat.db.part_trunc_int ORDER BY id

query
SELECT id, _partition.value_trunc FROM test_cat.db.part_trunc_int ORDER BY id

statement
DROP TABLE test_cat.db.part_trunc_int

-- =============================================================
-- Truncate transform: truncate(N, string_col)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_trunc_str

statement
CREATE TABLE test_cat.db.part_trunc_str (id INT, city STRING, data STRING) USING iceberg PARTITIONED BY (truncate(3, city))

statement
INSERT INTO test_cat.db.part_trunc_str VALUES (1, 'San Francisco', 'x'), (2, 'Santa Cruz', 'y'), (3, 'Boston', 'z')

query
SELECT id, _partition FROM test_cat.db.part_trunc_str ORDER BY id

query
SELECT id, _partition.city_trunc FROM test_cat.db.part_trunc_str ORDER BY id

statement
DROP TABLE test_cat.db.part_trunc_str

-- =============================================================
-- Multiple non-identity transforms on same table
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_multi_transform

statement
CREATE TABLE test_cat.db.part_multi_transform (id INT, name STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY (bucket(4, id), truncate(2, name), hours(ts))

statement
INSERT INTO test_cat.db.part_multi_transform VALUES (1, 'Alice', TIMESTAMP '2023-06-15 10:00:00'), (2, 'Bob', TIMESTAMP '2023-06-15 11:00:00'), (3, 'Alice', TIMESTAMP '2023-06-15 10:30:00')

query
SELECT id, _partition FROM test_cat.db.part_multi_transform ORDER BY id

query
SELECT id, _partition.id_bucket, _partition.name_trunc, _partition.ts_hour FROM test_cat.db.part_multi_transform ORDER BY id

statement
DROP TABLE test_cat.db.part_multi_transform

-- =============================================================
-- NULL partition values
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_nulls

statement
CREATE TABLE test_cat.db.part_nulls (id INT, dept STRING, value DOUBLE) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_nulls VALUES (1, 'eng', 10.0), (2, NULL, 20.0), (3, 'sales', 30.0), (4, NULL, 40.0)

query
SELECT id, _partition FROM test_cat.db.part_nulls ORDER BY id

query
SELECT id, _partition.dept FROM test_cat.db.part_nulls ORDER BY id

query
SELECT id, _partition FROM test_cat.db.part_nulls WHERE _partition.dept IS NULL ORDER BY id

query
SELECT id, _partition FROM test_cat.db.part_nulls WHERE _partition.dept IS NOT NULL ORDER BY id

statement
DROP TABLE test_cat.db.part_nulls

-- =============================================================
-- _partition in WHERE clause (filter on metadata column itself)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_where

statement
CREATE TABLE test_cat.db.part_where (id INT, dept STRING, value DOUBLE) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_where VALUES (1, 'eng', 10.0), (2, 'sales', 20.0), (3, 'eng', 30.0), (4, 'hr', 40.0)

query
SELECT id, value FROM test_cat.db.part_where WHERE _partition.dept = 'eng' ORDER BY id

query
SELECT id, _partition FROM test_cat.db.part_where WHERE _partition.dept IN ('eng', 'hr') ORDER BY id

statement
DROP TABLE test_cat.db.part_where

-- =============================================================
-- _partition in GROUP BY
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_groupby

statement
CREATE TABLE test_cat.db.part_groupby (id INT, dept STRING, salary DOUBLE) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_groupby VALUES (1, 'eng', 100.0), (2, 'eng', 120.0), (3, 'sales', 80.0), (4, 'sales', 90.0), (5, 'hr', 70.0)

query
SELECT _partition.dept, COUNT(*) as cnt, SUM(salary) as total FROM test_cat.db.part_groupby GROUP BY _partition.dept ORDER BY dept

-- ORDER BY on _partition (StructType) falls back because Comet does not support Sort on structs
query spark_answer_only
SELECT _partition, COUNT(*) as cnt FROM test_cat.db.part_groupby GROUP BY _partition ORDER BY _partition

statement
DROP TABLE test_cat.db.part_groupby

-- =============================================================
-- _partition in JOIN condition
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_join_left

statement
DROP TABLE IF EXISTS test_cat.db.part_join_right

statement
CREATE TABLE test_cat.db.part_join_left (id INT, dept STRING, value INT) USING iceberg PARTITIONED BY (dept)

statement
CREATE TABLE test_cat.db.part_join_right (id INT, dept STRING, budget INT) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_join_left VALUES (1, 'eng', 10), (2, 'sales', 20), (3, 'eng', 30)

statement
INSERT INTO test_cat.db.part_join_right VALUES (100, 'eng', 500), (200, 'sales', 300)

query
SELECT l.id, l.value, r.budget FROM test_cat.db.part_join_left l JOIN test_cat.db.part_join_right r ON l._partition.dept = r._partition.dept ORDER BY l.id

statement
DROP TABLE test_cat.db.part_join_left

statement
DROP TABLE test_cat.db.part_join_right

-- =============================================================
-- _partition in subquery
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_subq

statement
CREATE TABLE test_cat.db.part_subq (id INT, dept STRING, value INT) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_subq VALUES (1, 'eng', 10), (2, 'sales', 20), (3, 'hr', 30), (4, 'eng', 40)

query
SELECT id, value FROM test_cat.db.part_subq WHERE _partition.dept IN (SELECT _partition.dept FROM test_cat.db.part_subq WHERE value > 25) ORDER BY id

statement
DROP TABLE test_cat.db.part_subq

-- =============================================================
-- _partition with HAVING clause
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_having

statement
CREATE TABLE test_cat.db.part_having (id INT, dept STRING, salary DOUBLE) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_having VALUES (1, 'eng', 100.0), (2, 'eng', 120.0), (3, 'eng', 110.0), (4, 'sales', 80.0), (5, 'hr', 70.0)

query
SELECT _partition.dept, COUNT(*) as cnt FROM test_cat.db.part_having GROUP BY _partition.dept HAVING COUNT(*) > 1 ORDER BY dept

statement
DROP TABLE test_cat.db.part_having

-- =============================================================
-- _partition with ORDER BY on partition field
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_orderby

statement
CREATE TABLE test_cat.db.part_orderby (id INT, dept STRING, priority INT) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_orderby VALUES (1, 'eng', 3), (2, 'sales', 1), (3, 'hr', 2), (4, 'eng', 4)

query
SELECT id, _partition.dept FROM test_cat.db.part_orderby ORDER BY _partition.dept, id

statement
DROP TABLE test_cat.db.part_orderby

-- =============================================================
-- _partition with DISTINCT
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_distinct

statement
CREATE TABLE test_cat.db.part_distinct (id INT, region STRING, data STRING) USING iceberg PARTITIONED BY (region)

statement
INSERT INTO test_cat.db.part_distinct VALUES (1, 'US', 'a'), (2, 'EU', 'b'), (3, 'US', 'c'), (4, 'EU', 'd'), (5, 'APAC', 'e')

query spark_answer_only
SELECT DISTINCT _partition.region FROM test_cat.db.part_distinct ORDER BY region

query spark_answer_only
SELECT DISTINCT _partition FROM test_cat.db.part_distinct ORDER BY _partition.region

statement
DROP TABLE test_cat.db.part_distinct

-- =============================================================
-- Wide partition struct (many partition fields)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_wide

statement
CREATE TABLE test_cat.db.part_wide (id INT, a INT, b INT, c INT, d STRING, e STRING) USING iceberg PARTITIONED BY (a, b, c, d, e)

statement
INSERT INTO test_cat.db.part_wide VALUES (1, 10, 20, 30, 'x', 'y'), (2, 11, 21, 31, 'p', 'q'), (3, 10, 20, 30, 'x', 'y')

query
SELECT id, _partition FROM test_cat.db.part_wide ORDER BY id

query
SELECT id, _partition.a, _partition.b, _partition.c, _partition.d, _partition.e FROM test_cat.db.part_wide ORDER BY id

statement
DROP TABLE test_cat.db.part_wide

-- =============================================================
-- DATE identity partition
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_date_identity

statement
CREATE TABLE test_cat.db.part_date_identity (id INT, event_date DATE, data STRING) USING iceberg PARTITIONED BY (event_date)

statement
INSERT INTO test_cat.db.part_date_identity VALUES (1, DATE '2023-01-15', 'a'), (2, DATE '2023-02-20', 'b'), (3, DATE '2023-01-15', 'c')

query
SELECT id, _partition FROM test_cat.db.part_date_identity ORDER BY id

query
SELECT id, _partition.event_date FROM test_cat.db.part_date_identity ORDER BY id

query
SELECT id, data FROM test_cat.db.part_date_identity WHERE _partition.event_date = DATE '2023-01-15' ORDER BY id

statement
DROP TABLE test_cat.db.part_date_identity

-- =============================================================
-- String identity with special characters
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_special_chars

statement
CREATE TABLE test_cat.db.part_special_chars (id INT, tag STRING, data STRING) USING iceberg PARTITIONED BY (tag)

statement
INSERT INTO test_cat.db.part_special_chars VALUES (1, 'hello world', 'a'), (2, 'foo/bar', 'b'), (3, '', 'c'), (4, 'a=b&c=d', 'd')

query
SELECT id, _partition FROM test_cat.db.part_special_chars ORDER BY id

query
SELECT id, _partition.tag FROM test_cat.db.part_special_chars ORDER BY id

query
SELECT id FROM test_cat.db.part_special_chars WHERE _partition.tag = 'hello world' ORDER BY id

query
SELECT id FROM test_cat.db.part_special_chars WHERE _partition.tag = '' ORDER BY id

statement
DROP TABLE test_cat.db.part_special_chars

-- =============================================================
-- Bucket on STRING column
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_bucket_str

statement
CREATE TABLE test_cat.db.part_bucket_str (id INT, name STRING, value INT) USING iceberg PARTITIONED BY (bucket(8, name))

statement
INSERT INTO test_cat.db.part_bucket_str VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30), (4, 'Alice', 40)

query
SELECT id, _partition FROM test_cat.db.part_bucket_str ORDER BY id

query
SELECT id, _partition.name_bucket FROM test_cat.db.part_bucket_str ORDER BY id

statement
DROP TABLE test_cat.db.part_bucket_str

-- =============================================================
-- years() on TIMESTAMP column
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_years_ts

statement
CREATE TABLE test_cat.db.part_years_ts (id INT, ts TIMESTAMP, data STRING) USING iceberg PARTITIONED BY (years(ts))

statement
INSERT INTO test_cat.db.part_years_ts VALUES (1, TIMESTAMP '2021-03-15 08:00:00', 'a'), (2, TIMESTAMP '2022-07-20 12:00:00', 'b'), (3, TIMESTAMP '2021-11-01 16:00:00', 'c')

query
SELECT id, _partition FROM test_cat.db.part_years_ts ORDER BY id

query
SELECT id, _partition.ts_year FROM test_cat.db.part_years_ts ORDER BY id

statement
DROP TABLE test_cat.db.part_years_ts

-- =============================================================
-- months() on TIMESTAMP column
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_months_ts

statement
CREATE TABLE test_cat.db.part_months_ts (id INT, ts TIMESTAMP, data STRING) USING iceberg PARTITIONED BY (months(ts))

statement
INSERT INTO test_cat.db.part_months_ts VALUES (1, TIMESTAMP '2023-01-10 09:00:00', 'jan'), (2, TIMESTAMP '2023-02-15 10:00:00', 'feb'), (3, TIMESTAMP '2023-01-25 11:00:00', 'jan2')

query
SELECT id, _partition FROM test_cat.db.part_months_ts ORDER BY id

query
SELECT id, _partition.ts_month FROM test_cat.db.part_months_ts ORDER BY id

statement
DROP TABLE test_cat.db.part_months_ts

-- =============================================================
-- Combined: temporal transform + identity on same table
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_mixed

statement
CREATE TABLE test_cat.db.part_mixed (id INT, event_date DATE, region STRING, data STRING) USING iceberg PARTITIONED BY (years(event_date), region)

statement
INSERT INTO test_cat.db.part_mixed VALUES (1, DATE '2022-03-01', 'US', 'a'), (2, DATE '2023-07-15', 'EU', 'b'), (3, DATE '2022-09-20', 'US', 'c'), (4, DATE '2023-01-10', 'US', 'd')

query
SELECT id, _partition FROM test_cat.db.part_mixed ORDER BY id

query
SELECT id, _partition.event_date_year, _partition.region FROM test_cat.db.part_mixed ORDER BY id

query
SELECT _partition.event_date_year, COUNT(*) as cnt FROM test_cat.db.part_mixed GROUP BY _partition.event_date_year ORDER BY event_date_year

query
SELECT id, data FROM test_cat.db.part_mixed WHERE _partition.region = 'US' ORDER BY id

statement
DROP TABLE test_cat.db.part_mixed

-- =============================================================
-- _partition after multiple inserts (different files, same partition)
-- =============================================================

statement
DROP TABLE IF EXISTS test_cat.db.part_multi_insert

statement
CREATE TABLE test_cat.db.part_multi_insert (id INT, dept STRING, value INT) USING iceberg PARTITIONED BY (dept)

statement
INSERT INTO test_cat.db.part_multi_insert VALUES (1, 'eng', 10), (2, 'sales', 20)

statement
INSERT INTO test_cat.db.part_multi_insert VALUES (3, 'eng', 30), (4, 'hr', 40)

statement
INSERT INTO test_cat.db.part_multi_insert VALUES (5, 'sales', 50)

query
SELECT id, _partition FROM test_cat.db.part_multi_insert ORDER BY id

query
SELECT _partition.dept, COUNT(DISTINCT _file) as file_cnt, COUNT(*) as row_cnt FROM test_cat.db.part_multi_insert GROUP BY _partition.dept ORDER BY dept

statement
DROP TABLE test_cat.db.part_multi_insert

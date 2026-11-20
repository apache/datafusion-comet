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

statement
CREATE TABLE test_avg(i int, l long, f float, d double, grp string) USING parquet

statement
INSERT INTO test_avg VALUES (1, 10, 1.5, 1.5, 'a'), (2, 20, 2.5, 2.5, 'a'), (3, 30, 3.5, 3.5, 'b'), (NULL, NULL, NULL, NULL, 'b'), (0, 0, 0.0, 0.0, 'a')

query tolerance=1e-6
SELECT avg(i), avg(l), avg(f), avg(d) FROM test_avg

query tolerance=1e-6
SELECT grp, avg(d) FROM test_avg GROUP BY grp ORDER BY grp

-- single-row group (count == 1)
query tolerance=1e-6
SELECT grp, avg(d) FROM test_avg WHERE i = 3 GROUP BY grp

-- byte and short input types
statement
CREATE TABLE test_avg_small(b tinyint, s smallint, grp string) USING parquet

statement
INSERT INTO test_avg_small VALUES (1, 100, 'a'), (2, 200, 'a'), (3, 300, 'b'), (NULL, NULL, 'b'), (-1, -100, 'a')

query tolerance=1e-6
SELECT avg(b), avg(s) FROM test_avg_small

query tolerance=1e-6
SELECT grp, avg(b), avg(s) FROM test_avg_small GROUP BY grp ORDER BY grp

-- all-NULL input returns NULL
statement
CREATE TABLE test_avg_all_null(v double, grp string) USING parquet

statement
INSERT INTO test_avg_all_null VALUES (NULL, 'a'), (NULL, 'a'), (NULL, 'b')

query
SELECT avg(v) FROM test_avg_all_null

query
SELECT grp, avg(v) FROM test_avg_all_null GROUP BY grp ORDER BY grp

-- empty input (no rows) returns NULL
statement
CREATE TABLE test_avg_empty(v double) USING parquet

query
SELECT avg(v) FROM test_avg_empty

-- NaN and infinity input on doubles
statement
CREATE TABLE test_avg_special(v double, grp string) USING parquet

statement
INSERT INTO test_avg_special VALUES
  (double('NaN'), 'nan_only'),
  (1.0, 'nan_only'),
  (double('Infinity'), 'pos_inf_only'),
  (1.0, 'pos_inf_only'),
  (double('Infinity'), 'mixed_inf'),
  (double('-Infinity'), 'mixed_inf'),
  (double('-Infinity'), 'neg_inf_only'),
  (-2.0, 'neg_inf_only')

query tolerance=1e-6
SELECT grp, avg(v) FROM test_avg_special GROUP BY grp ORDER BY grp

-- boundary integer values
statement
CREATE TABLE test_avg_bounds(l long, grp string) USING parquet

statement
INSERT INTO test_avg_bounds VALUES
  (9223372036854775807, 'maxes'),
  (9223372036854775807, 'maxes'),
  (-9223372036854775808, 'mins'),
  (-9223372036854775808, 'mins'),
  (9223372036854775807, 'mixed'),
  (-9223372036854775808, 'mixed')

query tolerance=1e-6
SELECT grp, avg(l) FROM test_avg_bounds GROUP BY grp ORDER BY grp

-- negative-only inputs
statement
CREATE TABLE test_avg_negative(d double) USING parquet

statement
INSERT INTO test_avg_negative VALUES (-1.5), (-2.5), (-3.5), (double('-0.0'))

query tolerance=1e-6
SELECT avg(d) FROM test_avg_negative

-- decimal column at higher precision
statement
CREATE TABLE test_avg_decimal(d decimal(20, 5), grp string) USING parquet

statement
INSERT INTO test_avg_decimal VALUES
  (10.50000, 'a'),
  (20.25000, 'a'),
  (NULL, 'a'),
  (-5.00000, 'b'),
  (0.00000, 'b'),
  (5.00000, 'b')

query
SELECT avg(d) FROM test_avg_decimal

query
SELECT grp, avg(d) FROM test_avg_decimal GROUP BY grp ORDER BY grp

-- count(*) and avg in the same query for cross-check
query tolerance=1e-6
SELECT count(d), avg(d) FROM test_avg_decimal

-- Wide decimal division and overflow use native hash aggregation in legacy mode.
statement
SET spark.sql.adaptive.enabled=false

statement
SET spark.comet.shuffle.mode=native

statement
SET spark.sql.execution.useObjectHashAggregateExec=true

statement
CREATE TABLE test_avg_wide_division(k int, v decimal(38,18)) USING parquet

statement
INSERT INTO test_avg_wide_division SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 9000000000000000), (1, 9000000000000000), (2, 100000000000000000)

-- The sum's rescale exceeds i128, but division by the count produces a valid average.
query
SELECT k, AVG(v) FROM test_avg_wide_division WHERE k = 1 GROUP BY k

-- TRY_AVG keeps the valid group and returns NULL for final result overflow in either ANSI mode.
query
SELECT k, TRY_AVG(v) FROM test_avg_wide_division GROUP BY k

-- Final result overflow returns NULL in legacy mode.
query
SELECT k, AVG(v) FROM test_avg_wide_division WHERE k = 2 GROUP BY k

-- Ungrouped maximum-precision AVG still needs Spark's unbounded aggregation buffer.
query expect_fallback(Ungrouped decimal AVG with a maximum-precision sum)
SELECT AVG(v) FROM test_avg_wide_division WHERE k = 1

-- collect_list selects ObjectHashAggregate, whose buffer can recover from the intermediate 1.2.
statement
CREATE TABLE test_avg_object(k int, v decimal(38,38)) USING parquet

statement
INSERT INTO test_avg_object SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 0.6), (1, 0.6), (1, -0.4)

query expect_fallback(Decimal AVG with a maximum-precision sum cannot match Spark's unbounded object aggregation buffer)
SELECT k, AVG(v), sort_array(collect_list(k)) FROM test_avg_object GROUP BY k

-- Input precision 25 used to require an incompatibility opt-in for rescaling (#7007).
statement
CREATE TABLE test_avg_rescale(k int, v24 decimal(24,2), v25 decimal(25,2)) USING parquet

statement
INSERT INTO test_avg_rescale VALUES (1, 1.5, 1.5), (1, 2.5, 2.5)

query
SELECT k, AVG(v24), AVG(v25) FROM test_avg_rescale GROUP BY k

query
SELECT AVG(v24), AVG(v25) FROM test_avg_rescale

-- DECIMAL(38,38) needs no rescale and remains native when grouped.
statement
CREATE TABLE test_avg_no_rescale(k int, v decimal(38,38)) USING parquet

statement
INSERT INTO test_avg_no_rescale VALUES (1, 0.2), (1, 0.4)

query
SELECT k, AVG(v) FROM test_avg_no_rescale GROUP BY k

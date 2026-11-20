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

-- The queries of array_null_short_circuit.sql with ANSI off. A cast of a malformed string then
-- returns NULL instead of raising, so the cast queries match Spark however the arguments are
-- evaluated, and the native planner drops the guard for them. A start of 0 still makes slice raise,
-- and a nondeterministic argument still advances on every row it sees, so those two shapes depend on
-- skipping the rows whose array is NULL in every mode.
-- https://github.com/apache/datafusion-comet/issues/6613

-- Config: spark.sql.ansi.enabled=false

statement
CREATE TABLE array_null_short_circuit_non_ansi(ai array<int>, s string, i string) USING parquet

statement
INSERT INTO array_null_short_circuit_non_ansi SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, 'bad', 'bad'),
  (array(1, 2), '2', '1'),
  (array(3), '7', '0'),
  (NULL, 'worse', 'worse'),
  (array(4, NULL, 5), '5', '2')
  AS v(ai, s, i)

query expect_native(array_contains)
SELECT array_contains(ai, CAST(s AS INT)) FROM array_null_short_circuit_non_ansi

query expect_native(array_position)
SELECT array_position(ai, CAST(s AS INT)) FROM array_null_short_circuit_non_ansi

query expect_native(array_remove)
SELECT array_remove(ai, CAST(s AS INT)) FROM array_null_short_circuit_non_ansi

query expect_native(arrays_overlap)
SELECT arrays_overlap(ai, array(CAST(s AS INT))) FROM array_null_short_circuit_non_ansi

query expect_native(array_union)
SELECT array_union(ai, array(CAST(s AS INT))) FROM array_null_short_circuit_non_ansi

query expect_native(slice)
SELECT slice(ai, CAST(s AS INT), 1), slice(ai, 1, CAST(i AS INT)) FROM array_null_short_circuit_non_ansi

query expect_native(getarrayitem)
SELECT ai[CAST(i AS INT)] FROM array_null_short_circuit_non_ansi

-- A start of 0 makes slice raise in any mode, and only rows whose array is NULL have one
query expect_native(array_union)
SELECT array_union(ai, slice(array(1), IF(ai IS NULL, 0, 1), 1)) FROM array_null_short_circuit_non_ansi

-- A nondeterministic argument sees the same rows as in Spark, so it returns the same values
query expect_native(array_position)
SELECT array_position(ai, CAST(rand(7L) * 3 AS INT)) FROM array_null_short_circuit_non_ansi

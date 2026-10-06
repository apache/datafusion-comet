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

-- These array functions are null-intolerant BinaryExpressions or TernaryExpressions in Spark,
-- which return NULL for a NULL array without evaluating the other arguments. Comet evaluates each
-- later argument only for the rows where every earlier one is not NULL, so an argument that raises,
-- here an ANSI cast of a malformed string, cannot raise on a row Spark skips. Each query names the
-- expression with expect_native, because the codegen dispatcher would pass by running Spark's code.
-- https://github.com/apache/datafusion-comet/issues/6613

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE array_null_short_circuit(ai array<int>, s string, i string) USING parquet

-- `s` and `i` are malformed only where the array is NULL. COALESCE(1) writes a single file, so the
-- NULL arrays share a batch with the others.
statement
INSERT INTO array_null_short_circuit SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, 'bad', 'bad'),
  (array(1, 2), '2', '1'),
  (array(3), '7', '0'),
  (NULL, 'worse', 'worse'),
  (array(4, NULL, 5), '5', '2')
  AS v(ai, s, i)

query expect_native(array_contains)
SELECT array_contains(ai, CAST(s AS INT)) FROM array_null_short_circuit

query expect_native(array_position)
SELECT array_position(ai, CAST(s AS INT)) FROM array_null_short_circuit

query expect_native(array_remove)
SELECT array_remove(ai, CAST(s AS INT)) FROM array_null_short_circuit

query expect_native(arrays_overlap)
SELECT arrays_overlap(ai, array(CAST(s AS INT))) FROM array_null_short_circuit

query expect_native(array_union)
SELECT array_union(ai, array(CAST(s AS INT))) FROM array_null_short_circuit

-- slice is a TernaryExpression: the length is evaluated only where the start is not NULL either
query expect_native(slice)
SELECT slice(ai, CAST(s AS INT), 1), slice(ai, 1, CAST(i AS INT)) FROM array_null_short_circuit

query expect_native(getarrayitem)
SELECT ai[CAST(i AS INT)] FROM array_null_short_circuit

-- A start of 0 makes slice raise in any mode, and only rows whose array is NULL have one
query expect_native(array_union)
SELECT array_union(ai, slice(array(1), IF(ai IS NULL, 0, 1), 1)) FROM array_null_short_circuit

-- A nondeterministic argument sees the same rows as in Spark, so it returns the same values
query expect_native(array_position)
SELECT array_position(ai, CAST(rand(7L) * 3 AS INT)) FROM array_null_short_circuit

-- Spark's subexpression elimination evaluates a subexpression that an operator's expressions share
-- for every row, before them, so this cast raises on the rows whose array is NULL as well, in a
-- projection and in an aggregation. Comet does not skip an argument that holds one.
query expect_error(CAST_INVALID_INPUT)
SELECT array_contains(ai, CAST(s AS INT)), array_position(ai, CAST(s AS INT))
FROM array_null_short_circuit

query expect_error(CAST_INVALID_INPUT)
SELECT sum(array_position(ai, CAST(s AS INT))), max(array_contains(ai, CAST(s AS INT)))
FROM array_null_short_circuit

-- Here the whole call is shared, and Spark evaluates its cast only where the array is not NULL
query expect_native(array_contains)
SELECT array_contains(ai, CAST(s AS INT)), NOT array_contains(ai, CAST(s AS INT))
FROM array_null_short_circuit

-- Where the array is not NULL, the argument is evaluated and raises as in Spark
query expect_error(CAST_INVALID_INPUT)
SELECT array_contains(ai, CAST(s || 'x' AS INT)) FROM array_null_short_circuit

query expect_error(CAST_INVALID_INPUT)
SELECT ai[CAST(i || 'x' AS INT)] FROM array_null_short_circuit

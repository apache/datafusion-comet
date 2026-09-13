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

-- ANSI mode element_at tests
-- Tests that element_at throws exceptions for out-of-bounds access in ANSI mode
-- Note: element_at uses 1-based indexing

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Test data setup
-- ============================================================================

statement
CREATE TABLE ansi_element_at_oob(arr array<int>) USING parquet

statement
INSERT INTO ansi_element_at_oob VALUES (array(1, 2, 3))

-- Valid positive and negative boundary indices must run natively and match Spark.
query
SELECT element_at(arr, 1), element_at(arr, 3), element_at(arr, -1), element_at(arr, -3)
FROM ansi_element_at_oob

-- A NULL index returns NULL even in ANSI mode.
query
SELECT element_at(arr, CAST(NULL AS INT)) FROM ansi_element_at_oob

-- ============================================================================
-- element_at index out of bounds (positive index)
-- Spark and Comet throw INVALID_ARRAY_INDEX_IN_ELEMENT_AT in ANSI mode.
-- ============================================================================

-- index beyond array length should throw (1-based indexing)
query expect_error([INVALID_ARRAY_INDEX_IN_ELEMENT_AT])
SELECT element_at(arr, 4) FROM ansi_element_at_oob

query expect_error([INVALID_ARRAY_INDEX_IN_ELEMENT_AT])
SELECT element_at(arr, 10) FROM ansi_element_at_oob

-- literal array with out of bounds access
query expect_error([INVALID_ARRAY_INDEX_IN_ELEMENT_AT])
SELECT element_at(array(1, 2, 3), 5) FROM ansi_element_at_oob

-- ============================================================================
-- element_at with index 0 (invalid)
-- Spark and Comet throw INVALID_INDEX_OF_ZERO.
-- ============================================================================

-- index 0 is not valid for element_at (1-based indexing)
query expect_error([INVALID_INDEX_OF_ZERO])
SELECT element_at(arr, 0) FROM ansi_element_at_oob

-- literal with index 0
query expect_error([INVALID_INDEX_OF_ZERO])
SELECT element_at(array(1, 2, 3), 0) FROM ansi_element_at_oob

-- ============================================================================
-- element_at index out of bounds (negative index beyond array)
-- ============================================================================

-- negative index beyond array size should throw
query expect_error([INVALID_ARRAY_INDEX_IN_ELEMENT_AT])
SELECT element_at(arr, -4) FROM ansi_element_at_oob

query expect_error([INVALID_ARRAY_INDEX_IN_ELEMENT_AT])
SELECT element_at(arr, -10) FROM ansi_element_at_oob

-- literal with negative out of bounds
query expect_error([INVALID_ARRAY_INDEX_IN_ELEMENT_AT])
SELECT element_at(array(1, 2, 3), -5) FROM ansi_element_at_oob

-- ============================================================================
-- ANSI short-circuit over a NULL array
-- ============================================================================

statement
CREATE TABLE ansi_element_at_null(id int) USING parquet

statement
INSERT INTO ansi_element_at_null VALUES (1), (2), (3)

-- Spark's ElementAt is a BinaryExpression that returns NULL for a NULL array WITHOUT evaluating the
-- index, so the ANSI remainder-by-zero at id = 2 must not fire. CometElementAt reproduces that with
-- a `CASE WHEN <array> IS NOT NULL` guard, which runs the index only on the selected rows, so this
-- executes natively and returns 1, NULL, 1. The map counterpart lives in
-- map/element_at_map_ansi.sql.
query
SELECT id, element_at(IF(id <> 2, array(1), CAST(NULL AS ARRAY<INT>)), 1 + (id % (id - 2))) AS v
FROM ansi_element_at_null

-- Same guard over a `CASE WHEN` operand with no ELSE, which reaches the serde with an implicit NULL
-- branch. The index is a plain literal on purpose: with a throwing index, plain Spark 3.5 and 4.0
-- raise DIVIDE_BY_ZERO from this spelling on the very row whose array is NULL, contradicting their
-- own `BinaryExpression.eval` and Spark 4.1, so no single expected result covers every supported
-- version. The throwing-index case is covered by the `IF(...)` spelling above. Returns 1, NULL, 1.
query
SELECT id, element_at(CASE WHEN id <> 2 THEN array(1) END, 1) AS v
FROM ansi_element_at_null

-- A nondeterministic operand is dispatched as a whole. Neither native shape reproduces Spark: the
-- `CASE WHEN <array> IS NOT NULL` guard serializes the operand twice, so a stateful operand's two
-- copies drift, and the unguarded lookup evaluates the index over the whole batch, raising
-- DIVIDE_BY_ZERO at id = 2 on the very row whose array is NULL. The first operand alternates
-- NULL/non-NULL by row. `rand(7L) < 2` is always true, so the second operand is always NULL and
-- Spark returns NULL without evaluating its throwing index.
-- The non-ANSI spelling stays native and is covered in element_at.sql.
-- https://github.com/apache/datafusion-comet/issues/5544
query expect_dispatch(element_at)
SELECT id,
       element_at(IF(monotonically_increasing_id() % 2 = 0, CAST(NULL AS ARRAY<INT>), array(1)), 1) AS v1,
       element_at(IF(rand(7L) < 2, CAST(NULL AS ARRAY<INT>), array(1)), 1 + (id % (id - 2))) AS v2
FROM ansi_element_at_null

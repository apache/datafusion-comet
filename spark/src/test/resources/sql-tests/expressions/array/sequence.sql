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

-- sequence(start, stop[, step]) for integral element types runs on the native kernel
-- (https://github.com/apache/datafusion-comet/issues/5349). Date and timestamp sequences
-- stay on the JVM codegen dispatcher and are exercised at the bottom of this file.

statement
CREATE TABLE test_sequence(
  b_start tinyint, b_stop tinyint, b_step tinyint,
  s_start smallint, s_stop smallint, s_step smallint,
  i_start int, i_stop int, i_step int,
  l_start bigint, l_stop bigint, l_step bigint)
USING parquet

-- Row 2 descends, row 3 has start == stop, rows 4-6 carry NULLs in each argument position.
statement
INSERT INTO test_sequence VALUES
  (1Y, 5Y, 1Y, 1S, 5S, 1S, 1, 10, 3, 1L, 5L, 2L),
  (-3Y, -1Y, 1Y, 100S, 90S, -2S, 20, 2, -6, 9223372036854775802L, 9223372036854775807L, 1L),
  (0Y, 0Y, 0Y, -5S, -5S, 0S, 7, 7, 0, -9223372036854775808L, -9223372036854775800L, 3L),
  (NULL, 5Y, 1Y, NULL, 5S, 1S, NULL, 10, 1, NULL, 5L, 1L),
  (1Y, NULL, 1Y, 1S, NULL, 1S, 1, NULL, 1, 1L, NULL, 1L),
  (1Y, 5Y, NULL, 1S, 5S, NULL, 1, 10, NULL, 1L, 5L, NULL)

-- ============================================================================
-- Explicit step, all four integral types
-- ============================================================================

query
SELECT sequence(i_start, i_stop, i_step) FROM test_sequence

query
SELECT sequence(l_start, l_stop, l_step) FROM test_sequence

-- Column step for the narrow integral types exercises the Byte/Short monomorphizations
-- of the native kernel, not just the literal-step shape.
query
SELECT sequence(b_start, b_stop, b_step) FROM test_sequence

query
SELECT sequence(s_start, s_stop, s_step) FROM test_sequence

-- ============================================================================
-- Default step: per-row start <= stop ? 1 : -1, both directions in one column
-- ============================================================================

query
SELECT sequence(b_start, b_stop), sequence(s_start, s_stop) FROM test_sequence

query
SELECT sequence(i_start, i_stop), sequence(l_start, l_stop) FROM test_sequence

-- ============================================================================
-- Literal and mixed literal/column arguments
-- ============================================================================

query
SELECT sequence(1, 10), sequence(10, 1), sequence(5, 5), sequence(5, 5, 0)

query
SELECT sequence(1, 5), sequence(5, 1, -1), sequence(1, 10, 2)

query
SELECT sequence(1L, 9L, 2L), sequence(-128Y, -120Y), sequence(32760S, 32767S)

-- On row 2 the source row is (i_start=20, i_stop=2, i_step=-6), so the literal-step column
-- asks for sequence(1, 2, 2) = [1] while the default-step column asks for sequence(20, 25)
-- = [20, 21, 22, 23, 24, 25]. The two columns disagreeing in direction on the same row is
-- intentional coverage, not an oversight.
query
SELECT sequence(1, i_stop, 2), sequence(i_start, 25) FROM test_sequence WHERE i_start IS NOT NULL AND i_stop IS NOT NULL

query
SELECT sequence(CAST(NULL AS int), 5), sequence(1, CAST(NULL AS int)), sequence(1, 5, CAST(NULL AS int))

-- Integer.MIN_VALUE/MAX_VALUE bounds for int, and a sequence spanning zero
query
SELECT sequence(2147483642, 2147483647), sequence(-2147483648, -2147483643), sequence(-3, 3, 3)

-- ============================================================================
-- sequence feeding explode, the common date-spine shape (with integers)
-- ============================================================================

query
SELECT i_start, x FROM test_sequence LATERAL VIEW explode(sequence(i_start, i_stop)) AS x WHERE i_start IS NOT NULL AND i_stop IS NOT NULL

-- ============================================================================
-- Error paths: step direction contradicts bounds, or zero step with start != stop
-- ============================================================================

query expect_error(Illegal sequence boundaries: 1 to 5 by -1)
SELECT sequence(1, 5, -1)

query expect_error(Illegal sequence boundaries: 10 to 2 by 3)
SELECT sequence(10, 2, 3) FROM test_sequence LIMIT 1

query expect_error(Illegal sequence boundaries: 1 to 5 by 0)
SELECT sequence(1, 5, 0)

-- ============================================================================
-- Error paths: length exceeds MAX_ROUNDED_ARRAY_LENGTH
-- ============================================================================

query expect_error(the array size limit 2147483632)
SELECT sequence(0L, 4294967296L, 1L)

-- Math.addExact overflow inside Spark's sequenceLength: reported count is 2^63
query expect_error(9223372036854775808)
SELECT sequence(0L, 9223372036854775807L, 1L)

-- Long.MinValue / -1 special case: reported count is 2^63 + 1
query expect_error(9223372036854775809)
SELECT sequence(0L, -9223372036854775808L, -1L)

-- delta overflows long but the exact length is tiny: Spark reaches an internal error
query expect_error(Unreachable code reached)
SELECT sequence(-9223372036854775808L, 9223372036854775807L, 9223372036854775807L)

-- ============================================================================
-- Full narrow-type range: writes at the byte/short boundary. Spark's kernel
-- accumulates with the element type's `Numeric`, wrapping at 8 and 16 bits;
-- ours accumulates in i64 and truncates on the way out. The two agree because
-- every element is inside range, but this locks in the boundary values.
-- ============================================================================

query
SELECT sequence(-128Y, 127Y), sequence(-32768S, 32767S)

-- ============================================================================
-- Int32 boundary product: index * step overflows int, exercising the Int32
-- monomorphization at the extreme.
-- ============================================================================

query
SELECT sequence(-2147483648, 2147483647, 1073741824)

-- ============================================================================
-- Null short-circuit under a nested sequence: Spark's codegen returns NULL
-- without evaluating the inner argument, so the inner `sequence(1, 5, -1)`
-- must not fire on the NULL row. Non-leaf argument shapes stay on the JVM
-- codegen dispatcher for this reason
-- (https://github.com/apache/datafusion-comet/pull/5614#discussion_r3910237757).
-- ============================================================================

statement
CREATE TABLE t_seq_null_short_circuit(s INT, k INT) USING parquet

statement
INSERT INTO t_seq_null_short_circuit VALUES (NULL, -1), (1, 1)

query
SELECT sequence(s, size(sequence(1, 5, k))) FROM t_seq_null_short_circuit

-- ============================================================================
-- Throwing sub-expression guarded by CASE WHEN: DataFusion filters the batch
-- per branch, so `sequence(1, 5, k)` is never evaluated on rows where the
-- ELSE branch is taken. Locks in that we do not diverge from Spark here.
-- ============================================================================

query
SELECT CASE WHEN k > 0 THEN sequence(1, 5, k) ELSE array(-1) END FROM t_seq_null_short_circuit

-- ============================================================================
-- Date and timestamp sequences keep running on the JVM codegen dispatcher
-- ============================================================================

query
SELECT sequence(DATE'2024-01-01', DATE'2024-01-10')

query
SELECT sequence(DATE'2024-01-01', DATE'2024-12-31', INTERVAL 1 MONTH)

query
SELECT sequence(TIMESTAMP'2024-01-01 00:00:00', TIMESTAMP'2024-01-01 06:00:00', INTERVAL 2 HOUR)

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

-- ConfigMatrix: spark.comet.exec.scalaUDF.codegen.enabled=true,false
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.sql.ansi.enabled=true

statement
SET spark.comet.enabled=false

statement
CREATE TABLE conditional_pad_subq USING parquet AS
SELECT id, p, s, CAST(NULL AS STRING) AS nr, CAST(NULL AS ARRAY<STRING>) AS na
FROM VALUES (0, true, 'a'), (1, true, 'b') AS rows(id, p, s)

statement
SET spark.comet.enabled=true

-- A NULL string must short-circuit an ANSI length cast that would throw.
query expect_native(lpad)
SELECT lpad(concat_ws(CAST(NULL AS STRING), s), CAST(s AS INT), '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

query expect_native(rpad)
SELECT rpad(concat_ws(CAST(NULL AS STRING), s), CAST(s AS INT), '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- A NULL concat_ws separator can become a scalar even with column arguments.
query expect_native(lpad)
SELECT lpad(concat_ws(CAST(NULL AS STRING), s), 8, '0'),
       rpad(concat_ws(CAST(NULL AS STRING), s), 8, '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- References to p and s do not imply an array result: every row selects the
-- scalar subquery branch, and max/min merge into a struct scalar subquery.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT lpad(IF(p, (SELECT max(s) FROM conditional_pad_subq), s), 8, '0'),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT rpad(IF(p, (SELECT max(s) FROM conditional_pad_subq), s), 8, '0'),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- The scalar ELSE branch and CASE WHEN must receive the same protection.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT lpad(IF(NOT p, s, (SELECT max(s) FROM conditional_pad_subq)), 8, '0'),
       rpad(CASE WHEN p THEN (SELECT min(s) FROM conditional_pad_subq) ELSE s END, 8, '0')
FROM conditional_pad_subq

-- A mixed batch may produce an array, but other batches of the same expression
-- can select one scalar branch. Planning must not depend on the first batch.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, lpad(IF(id = 0, (SELECT max(s) FROM conditional_pad_subq), s), 8, '0'),
       rpad(IF(id = 0, (SELECT min(s) FROM conditional_pad_subq), s), 8, '0')
FROM conditional_pad_subq

-- Wrappers cannot hide a conditional subquery input, including runtime NULLs.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT lpad(concat(IF(p, (SELECT max(s) FROM conditional_pad_subq), s), ''), 8, '0'),
       rpad(IF(p, (SELECT min(s) FROM conditional_pad_subq WHERE id < 0), s), 8, '0')
FROM conditional_pad_subq

-- A non-merged scalar subquery must be safe too.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT rpad(IF(p, (SELECT max(s) FROM conditional_pad_subq), s), 8, '0')
FROM conditional_pad_subq

-- Only the string argument is guarded: column strings and runtime subquery
-- lengths, including NULL lengths, must retain their native kernels.
query expect_native(lpad)
SELECT lpad(s, (SELECT max(id) + 8 FROM conditional_pad_subq), '0'),
       (SELECT min(id) FROM conditional_pad_subq)
FROM conditional_pad_subq

query expect_native(rpad)
SELECT rpad(s, (SELECT max(id) FROM conditional_pad_subq WHERE id < 0), '0'),
       (SELECT min(id) FROM conditional_pad_subq WHERE id < 0)
FROM conditional_pad_subq

-- The merged struct can be in a sibling projection. Constant conditional
-- branches still produce runtime scalars, handled by the native array adapter.
query expect_native(lpad)
SELECT lpad(IF(p, 'x', s), 8, '0'), rpad(IF(p, 'y', s), 8, '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- A missing ELSE is an implicit scalar NULL when no row selects a branch.
query expect_native(lpad)
SELECT lpad(CASE WHEN NOT p THEN s END, 8, '0'),
       rpad(CASE WHEN p THEN 'x' ELSE s END, 8, '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- Conditions whose value branches are columns still supply arrays to padding.
query expect_native(lpad)
SELECT lpad(IF(p, s, reverse(s)), 8, '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- Coalesce is serialized as CASE and can select a constant for the whole batch.
query expect_native(lpad)
SELECT lpad(coalesce(CASE WHEN NOT p THEN s END, 'x'), 8, '0'),
       rpad(coalesce(CASE WHEN NOT p THEN s END, 'y'), 8, '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- Serdes can inject conditional scalar results not visible in the Spark expression.
query expect_native(lpad)
SELECT lpad(array_join(array(s), ',', nr), 8, '0'),
       rpad(array_join(array(s), ',', nr), CAST(id + 7 AS INT), '0'),
       lpad(CAST(size(na) AS STRING), 8, '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- A homogeneous scalar branch still broadcasts against per-row lengths.
query expect_native(lpad)
SELECT id, lpad(IF(p, 'é😊', s), CAST(id + 3 AS INT), '界'),
       rpad(IF(p, 'é😊', s), CAST(id + 3 AS INT), '界'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

-- Mixed rows must skip malformed lengths only where the string is NULL.
statement
SET spark.comet.enabled=false

statement
CREATE TABLE padding_null_short_circuit(s string, len_text string) USING parquet

statement
INSERT INTO padding_null_short_circuit SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, 'bad'), ('x', '4'), (NULL, 'worse'), ('ab', '3') AS v(s, len_text)

statement
SET spark.comet.enabled=true

query expect_native(lpad)
SELECT lpad(s, CAST(len_text AS INT), '0'),
       (SELECT max(s) FROM padding_null_short_circuit),
       (SELECT min(s) FROM padding_null_short_circuit)
FROM padding_null_short_circuit

query expect_native(rpad)
SELECT rpad(s, CAST(len_text AS INT), '0'),
       (SELECT max(s) FROM padding_null_short_circuit),
       (SELECT min(s) FROM padding_null_short_circuit)
FROM padding_null_short_circuit

-- A malformed length on a non-NULL row still raises.
query expect_error(CAST_INVALID_INPUT)
SELECT lpad(s, CAST(len_text || 'x' AS INT), '0') FROM padding_null_short_circuit

-- Spark evaluates a shared cast eagerly; the guard must preserve that error.
query expect_error(CAST_INVALID_INPUT)
SELECT lpad(concat_ws(CAST(NULL AS STRING), s), CAST(s AS INT), '0'),
       rpad(concat_ws(CAST(NULL AS STRING), s), CAST(s AS INT), '0'),
       (SELECT max(s) FROM conditional_pad_subq),
       (SELECT min(s) FROM conditional_pad_subq)
FROM conditional_pad_subq

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

-- With spark.sql.subexpressionElimination.enabled off, Spark does not evaluate a subexpression
-- that an operator's expressions share ahead of them, so a null-intolerant function skips a later
-- argument holding one on a row whose array is NULL, and Comet skips it too. Spark's generated
-- code for an aggregate shares subexpressions whatever the setting says, so there the cast still
-- raises. The same queries with the setting on are in array_null_short_circuit.sql.
-- https://github.com/apache/datafusion-comet/issues/6972

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=false
-- Config: spark.sql.codegen.wholeStage=true

statement
CREATE TABLE array_null_short_circuit_cse_off(ai array<int>, s string, i string) USING parquet

-- `s` is malformed only where the array is NULL. COALESCE(1) writes a single file, so the NULL
-- arrays share a batch with the others.
statement
INSERT INTO array_null_short_circuit_cse_off SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, 'bad', 'bad'),
  (array(1, 2), '2', '1'),
  (array(3), '7', '0'),
  (NULL, 'worse', 'worse'),
  (array(4, NULL, 5), '5', '2')
  AS v(ai, s, i)

-- A projection's expressions share the cast
query expect_native(array_contains, array_position)
SELECT array_contains(ai, CAST(s AS INT)), array_position(ai, CAST(s AS INT))
FROM array_null_short_circuit_cse_off

-- So do a shuffle's partitioning keys, for hash and range partitioning, and a
-- TakeOrderedAndProject's projection list
query
SELECT ai, s FROM array_null_short_circuit_cse_off
DISTRIBUTE BY array_contains(ai, CAST(s AS INT)), array_position(ai, CAST(s AS INT))

query
SELECT ai, s FROM array_null_short_circuit_cse_off
ORDER BY array_contains(ai, CAST(s AS INT)), array_position(ai, CAST(s AS INT)), s

query expect_native(array_contains, array_position)
SELECT array_contains(ai, CAST(s AS INT)), array_position(ai, CAST(s AS INT))
FROM (SELECT * FROM array_null_short_circuit_cse_off ORDER BY i LIMIT 10)

-- An aggregate's update expressions still share the cast, with or without grouping keys
query expect_error_class(CAST_INVALID_INPUT)
SELECT sum(array_position(ai, CAST(s AS INT))), max(array_contains(ai, CAST(s AS INT)))
FROM array_null_short_circuit_cse_off

query expect_error_class(CAST_INVALID_INPUT)
SELECT i, sum(array_position(ai, CAST(s AS INT))), max(array_contains(ai, CAST(s AS INT)))
FROM array_null_short_circuit_cse_off GROUP BY i

-- Where the whole call is shared rather than the cast, the call skips the cast on its own
query expect_native(array_position)
SELECT sum(array_position(ai, CAST(s AS INT))), max(array_position(ai, CAST(s AS INT)))
FROM array_null_short_circuit_cse_off

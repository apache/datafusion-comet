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

-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.batchSize=8192
-- Config: spark.sql.ansi.enabled=true

statement
SET spark.comet.enabled=false

statement
CREATE TABLE large_field_source USING parquet AS
SELECT id, repeat(CASE WHEN id = 0 THEN 'a' ELSE 'b' END, 262144) AS s
FROM range(0, 2, 1, 1)

statement
CREATE TABLE large_field_outer USING parquet AS
SELECT id FROM range(0, 8192, 1, 1)

statement
SET spark.comet.enabled=true

-- MergeScalarSubqueries produces a struct. Broadcasting either field first would require
-- 2^31 bytes in a Utf8 values buffer, even though the consumer only returns an integer.
query expect_native(octet_length)
SELECT id, octet_length((SELECT max(s) FROM large_field_source)),
       octet_length((SELECT min(s) FROM large_field_source))
FROM large_field_outer

-- Nested consumers must reduce the value before broadcasting any intermediate string.
query expect_native(octet_length,concat)
SELECT id, octet_length(concat((SELECT max(s) FROM large_field_source), 'x')),
       octet_length((SELECT min(s) FROM large_field_source))
FROM large_field_outer

-- The reduced result must still work in mixed and homogeneous conditional branches.
query expect_native(octet_length)
SELECT id, IF(id % 2 = 0, octet_length((SELECT max(s) FROM large_field_source)), 0),
       IF(id >= 0, octet_length((SELECT min(s) FROM large_field_source)), 0)
FROM large_field_outer

-- A column-dependent branch must be consumed on bounded slices, preserving row order and NULLs.
query expect_native(octet_length)
SELECT id, octet_length(IF(id + 1 > 0, (SELECT max(s) FROM large_field_source), '')),
       octet_length(IF(id % 2 = 0, (SELECT min(s) FROM large_field_source), NULL))
FROM large_field_outer

-- Binary expressions also need to consume the field before broadcasting it.
query expect_native(equalto)
SELECT id, (SELECT max(s) FROM large_field_source) = 'x',
       (SELECT min(s) FROM large_field_source) = 'x'
FROM large_field_outer

-- Stateful predicates stay on Spark: reslicing them is not an assumed native capability.
query expect_fallback(Nondeterministic consumers of variable-width struct scalar subqueries require Spark)
SELECT id, octet_length(IF(rand(42) >= 0, (SELECT max(s) FROM large_field_source), '')),
       octet_length((SELECT min(s) FROM large_field_source))
FROM large_field_outer

-- A dispatched predicate has no subquery itself, but its surrounding consumer does.
query expect_fallback(JVM-dispatched consumers of variable-width struct scalar subqueries require Spark)
SELECT id, octet_length(IF(upper(CAST(id AS STRING)) <> 'missing',
                          (SELECT max(s) FROM large_field_source), '')),
       octet_length((SELECT min(s) FROM large_field_source))
FROM large_field_outer

-- An independent plain scalar result is constant too, even though its JVM lookup is deferred.
query expect_native(octet_length)
SELECT id, octet_length(IF((SELECT count(*) FROM large_field_outer) > 0,
                          (SELECT max(s) FROM large_field_source), '')),
       octet_length((SELECT min(s) FROM large_field_source))
FROM large_field_outer

-- A runtime NULL scalar-subquery field remains NULL on every outer row.
query expect_native(octet_length)
SELECT id, octet_length((SELECT max(s) FROM large_field_source WHERE id < 0)),
       octet_length((SELECT min(s) FROM large_field_source WHERE id < 0))
FROM large_field_outer

-- Do not eagerly evaluate an ANSI cast inside an unselected branch. The predicate is a column
-- expression, so the optimizer cannot discard the failing branch as a constant IF.
query expect_native(octet_length)
SELECT id, IF(id < 0, octet_length(CAST(CAST((SELECT max(s) FROM large_field_source)
                                         AS INT) AS STRING)), 0),
       octet_length((SELECT min(s) FROM large_field_source))
FROM large_field_outer

statement
DROP TABLE large_field_source

statement
DROP TABLE large_field_outer

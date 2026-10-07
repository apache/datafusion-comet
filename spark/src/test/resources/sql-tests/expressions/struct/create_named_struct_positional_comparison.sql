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

-- MinSparkVersion: 4.0

-- Config: spark.comet.exec.range.enabled=true

-- Spark 4.x compares struct fields by position, irrespective of field names.
-- Equality and the ordering/null-safe predicates use different native comparison paths.
query expect_native(equalto,lessthan,equalnullsafe)
SELECT named_struct('x', CAST(id AS DOUBLE), 'y', CAST(NULL AS DOUBLE)) =
       named_struct('y', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE)),
       named_struct('x', CAST(id AS DOUBLE), 'y', CAST(NULL AS DOUBLE)) <
       named_struct('y', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE)),
       named_struct('x', CAST(id AS DOUBLE), 'y', CAST(NULL AS DOUBLE)) <=>
       named_struct('y', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE))
FROM range(8)

-- Equal nullability must not bypass field-name alignment. Native Range makes every field
-- non-nullable; id=1 compares equal, while id=0 detects an accidental name-based reorder.
query expect_native(equalto,lessthan,equalnullsafe)
SELECT named_struct('x', CAST(id AS DOUBLE), 'y', 1D) =
       named_struct('y', 1D, 'x', CAST(id AS DOUBLE)),
       named_struct('x', CAST(id AS DOUBLE), 'y', 1D) <
       named_struct('y', 1D, 'x', CAST(id AS DOUBLE)),
       named_struct('x', CAST(id AS DOUBLE), 'y', 1D) <=>
       named_struct('y', 1D, 'x', CAST(id AS DOUBLE)),
       array(named_struct('x', CAST(id AS DOUBLE), 'y', 1D)) =
       array(named_struct('y', 1D, 'x', CAST(id AS DOUBLE)))
FROM range(0, 3)

-- Scanned structs carry nullable parents and leaves. Keep distinct field values so a
-- name-based cast changes the answer, and include Spark's nested NaN/signed-zero semantics.
statement
CREATE TABLE test_struct_positional_comparison(
  id INT, a STRUCT<x:DOUBLE,y:DOUBLE>, b STRUCT<y:DOUBLE,x:DOUBLE>
) USING parquet

statement
INSERT INTO test_struct_positional_comparison VALUES
  (1, named_struct('x', 1D, 'y', 2D), named_struct('y', 1D, 'x', 2D)),
  (2, named_struct('x', 1D, 'y', 2D), named_struct('y', 2D, 'x', 1D)),
  (3, NULL, NULL),
  (4, NULL, named_struct('y', 1D, 'x', 2D)),
  (5, named_struct('x', CAST(NULL AS DOUBLE), 'y', 1D), named_struct('y', CAST(NULL AS DOUBLE), 'x', 1D)),
  (6, named_struct('x', CAST(NULL AS DOUBLE), 'y', 1D), named_struct('y', 0D, 'x', 1D)),
  (7, named_struct('x', CAST('-0.0' AS DOUBLE), 'y', 1D), named_struct('y', 0D, 'x', 1D)),
  (8, named_struct('x', CAST('NaN' AS DOUBLE), 'y', 1D), named_struct('y', CAST('NaN' AS DOUBLE), 'x', 1D)),
  (9, named_struct('x', CAST('NaN' AS DOUBLE), 'y', 1D), named_struct('y', 1D, 'x', 1D))

query expect_native(equalto)
SELECT id, a = b FROM test_struct_positional_comparison

-- Signed-zero ordering/null-safe equality is the separate #6157 gap, fixed upstream by #6447.
-- Keep this branch's positional regression independent of that fix; equality above covers zeros.
query expect_native(lessthan,equalnullsafe)
SELECT id, a < b, a <=> b FROM test_struct_positional_comparison WHERE id <> 7

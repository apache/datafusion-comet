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

-- With strict floating-point mode on, Comet's native float sort is Incompatible with Spark
-- (NaN / signed-zero ordering). CometSortArray mixes in CodegenDispatchFallback, so it routes
-- through the JVM codegen dispatcher and matches Spark exactly instead of falling back.

-- Config: spark.comet.exec.strictFloatingPoint=true

statement
CREATE TABLE test_sort_array_strict_double(arr array<double>) USING parquet

statement
INSERT INTO test_sort_array_strict_double VALUES
  (array(CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE), 3.0, 1.0, NULL, -0.0, 0.0)),
  (array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE), -5.0, 2.0)),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_strict_double

query
SELECT sort_array(arr, false) FROM test_sort_array_strict_double

statement
CREATE TABLE test_sort_array_strict_float(arr array<float>) USING parquet

statement
INSERT INTO test_sort_array_strict_float VALUES
  (array(CAST('Infinity' AS FLOAT), CAST('-Infinity' AS FLOAT), CAST('NaN' AS FLOAT), CAST(3.0 AS FLOAT), CAST(NULL AS FLOAT), CAST(-0.0 AS FLOAT), CAST(0.0 AS FLOAT))),
  (array()),
  (NULL)

query
SELECT sort_array(arr) FROM test_sort_array_strict_float

query
SELECT sort_array(arr, false) FROM test_sort_array_strict_float

-- nested float arrays also dispatch
query
SELECT sort_array(array(CAST('NaN' AS DOUBLE), -0.0, 0.0, 1.0, CAST('-Infinity' AS DOUBLE)))

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
CREATE TABLE test_array_min(arr array<int>) USING parquet

statement
INSERT INTO test_array_min VALUES (array(1, 2, 3)), (array(3, 1, 2)), (array()), (NULL), (array(NULL, 1, 2)), (array(-1, -2, -3))

query spark_answer_only
SELECT array_min(arr) FROM test_array_min

-- literal arguments
query
SELECT array_min(array(1, 2, 3)), array_min(array()), array_min(cast(NULL as array<int>))

-- ===== DOUBLE arrays with NaN/Infinity/-0.0 =====

statement
CREATE TABLE test_array_min_double(arr array<double>) USING parquet

statement
INSERT INTO test_array_min_double VALUES
  (array(CAST('NaN' AS DOUBLE), 1.0, 2.0)),
  (array(1.0, CAST('NaN' AS DOUBLE), 2.0)),
  (array(1.0, 2.0, CAST('NaN' AS DOUBLE))),
  (array(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))),
  (array(CAST('NaN' AS DOUBLE), NULL, 1.0)),
  (array(CAST('Infinity' AS DOUBLE), 1.0, 2.0)),
  (array(CAST('-Infinity' AS DOUBLE), 1.0, 2.0)),
  (array(CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), CAST('-Infinity' AS DOUBLE))),
  (array(0.0, -0.0, 1.0)),
  (NULL),
  (array())

query
SELECT array_min(arr) FROM test_array_min_double

-- ===== FLOAT arrays with NaN/Infinity/-0.0 =====

statement
CREATE TABLE test_array_min_float(arr array<float>) USING parquet

statement
INSERT INTO test_array_min_float VALUES
  (array(CAST('NaN' AS FLOAT), CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT))),
  (array(CAST('NaN' AS FLOAT), CAST('NaN' AS FLOAT))),
  (array(CAST('NaN' AS FLOAT), NULL, CAST(1.0 AS FLOAT))),
  (array(CAST('Infinity' AS FLOAT), CAST(1.0 AS FLOAT))),
  (array(CAST('-Infinity' AS FLOAT), CAST(1.0 AS FLOAT))),
  (array(CAST(0.0 AS FLOAT), CAST(-0.0 AS FLOAT))),
  (NULL),
  (array())

query
SELECT array_min(arr) FROM test_array_min_float

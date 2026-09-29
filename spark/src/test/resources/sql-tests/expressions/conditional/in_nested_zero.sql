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

-- ConfigMatrix: spark.sql.optimizer.inSetConversionThreshold=100,0

-- Nested membership must use Spark equality for signed zero and NaN.
statement
CREATE TABLE comet_nested_in_zero (
  id INT,
  a ARRAY<DOUBLE>,
  s STRUCT<v: DOUBLE>
) USING parquet

statement
INSERT INTO comet_nested_in_zero VALUES (
  1,
  array(CAST('-0.0' AS DOUBLE)),
  named_struct('v', CAST('-0.0' AS DOUBLE))
)

query
SELECT id, a IN (array(CAST('0.0' AS DOUBLE))) AS array_match
FROM comet_nested_in_zero

query
SELECT id, s IN (named_struct('v', CAST('0.0' AS DOUBLE))) AS struct_match
FROM comet_nested_in_zero

-- Multiple distinct candidates exercise In and InSet under the configuration matrix.
query
SELECT id, a IN (array(CAST('0.0' AS DOUBLE)), array(CAST('2.0' AS DOUBLE))),
  a NOT IN (array(CAST('0.0' AS DOUBLE)), array(CAST('2.0' AS DOUBLE))),
  s IN (named_struct('v', CAST('0.0' AS DOUBLE)), named_struct('v', CAST('2.0' AS DOUBLE)))
FROM comet_nested_in_zero

statement
CREATE TABLE nested_membership(a ARRAY<FLOAT>, b ARRAY<FLOAT>, n ARRAY<ARRAY<DOUBLE>>) USING parquet

statement
INSERT INTO nested_membership VALUES
  (array(CAST('-0.0' AS FLOAT)), array(CAST('0.0' AS FLOAT)), array(array(CAST('-0.0' AS DOUBLE)))),
  (array(CAST('0.0' AS FLOAT)), array(CAST('-0.0' AS FLOAT)), array(array(CAST('0.0' AS DOUBLE)))),
  (array(CAST('NaN' AS FLOAT)), array(CAST('NaN' AS FLOAT)), array(array(CAST('NaN' AS DOUBLE)))),
  (array(CAST(NULL AS FLOAT)), array(CAST(NULL AS FLOAT)), array(array(CAST(NULL AS DOUBLE)))),
  (array(CAST('1.0' AS FLOAT)), array(CAST('2.0' AS FLOAT)), array(array(CAST('3.0' AS DOUBLE)))),
  (array(), array(), array()),
  (NULL, NULL, NULL)

query expect_native(in)
SELECT a IN (b), a NOT IN (b) FROM nested_membership

query
SELECT a IN (array(CAST('0.0' AS FLOAT)), array(CAST('NaN' AS FLOAT))),
  n IN (array(array(CAST('0.0' AS DOUBLE))), array(array(CAST('NaN' AS DOUBLE))))
FROM nested_membership

-- Direct comparisons also cover the equality path used by OptimizeIn.
query expect_native(equalto)
SELECT a = b, a <> b FROM nested_membership

-- Unknown candidates must not hide a later match, and NOT IN preserves unknown.
query expect_native(in)
SELECT a IN (CAST(NULL AS ARRAY<FLOAT>), b),
  a NOT IN (CAST(NULL AS ARRAY<FLOAT>), b),
  a IN (array(CAST('0.0' AS FLOAT)), b),
  a NOT IN (array(CAST('0.0' AS FLOAT)), b)
FROM nested_membership

query
SELECT s = named_struct('v', CAST('0.0' AS DOUBLE)),
  s <> named_struct('v', CAST('0.0' AS DOUBLE))
FROM comet_nested_in_zero

query expect_native(equalto)
SELECT n = array(array(CAST('0.0' AS DOUBLE))),
  n <> array(array(CAST('0.0' AS DOUBLE)))
FROM nested_membership

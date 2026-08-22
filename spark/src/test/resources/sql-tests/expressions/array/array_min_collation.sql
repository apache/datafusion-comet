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
-- Non-binary collations still require Spark's ordering, including when nested.
-- Neither strict floating-point mode nor enabling the dispatcher may bypass this guard.
-- Config: spark.comet.expression.ArrayMin.allowIncompatible=false
-- ConfigMatrix: spark.comet.exec.strictFloatingPoint=false,true
-- ConfigMatrix: spark.comet.exec.scalaUDF.codegen.enabled=false,true

statement
CREATE TABLE test_array_min_collation(
  id int, a string, b string, x double, y double) USING parquet

statement
INSERT INTO test_array_min_collation VALUES
  (1, 'a', 'B', double('0.0'), double('-0.0')),
  (2, 'B', 'a', double('-0.0'), double('0.0')),
  (3, 'A', 'a', double('-0.0'), double('0.0')),
  (4, NULL, 'B', NULL, double('0.0')),
  (5, 'a', NULL, double('-0.0'), NULL),
  (6, NULL, NULL, NULL, NULL)

-- Binary string ordering remains native, including inside arrays and structs.
query
SELECT id, array_min(array(a, b)) FROM test_array_min_collation

query
SELECT id, array_min(array(array(a), array(b))),
       array_min(array(named_struct('s', a, 'f', x), named_struct('s', b, 'f', y)))
FROM test_array_min_collation

-- Lowercase ordering differs from binary ordering for the column values 'a' and 'B'.
query expect_fallback(Array extrema use binary string ordering)
SELECT id, array_min(array(
         CAST(a AS STRING COLLATE UTF8_LCASE),
         CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_min_collation

query expect_fallback(Array extrema use binary string ordering)
SELECT id, array_min(array(
         array(CAST(a AS STRING COLLATE UTF8_LCASE)),
         array(CAST(b AS STRING COLLATE UTF8_LCASE))))
FROM test_array_min_collation

-- A floating-point field does not make a collated struct eligible for native ordering.
query expect_fallback(Array extrema use binary string ordering)
SELECT id, array_min(array(
         named_struct('s', CAST(a AS STRING COLLATE UTF8_LCASE), 'f', x),
         named_struct('s', CAST(b AS STRING COLLATE UTF8_LCASE), 'f', y)))
FROM test_array_min_collation

-- The collation check must recurse through both struct fields and nested array elements.
query expect_fallback(Array extrema use binary string ordering)
SELECT id, array_min(array(
         named_struct('s', array(CAST(a AS STRING COLLATE UTF8_LCASE)), 'f', x),
         named_struct('s', array(CAST(b AS STRING COLLATE UTF8_LCASE)), 'f', y)))
FROM test_array_min_collation

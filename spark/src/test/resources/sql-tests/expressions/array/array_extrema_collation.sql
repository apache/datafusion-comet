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
-- Config: spark.comet.expression.ArrayMin.allowIncompatible=false
-- Config: spark.comet.expression.ArrayMax.allowIncompatible=false
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_array_extrema_collation(
  id int, a string, b string, x double, y double) USING parquet

statement
INSERT INTO test_array_extrema_collation VALUES
  (1, 'a', 'B', double('0.0'), double('-0.0')),
  (2, 'B', 'a', double('-0.0'), double('0.0')),
  (3, 'A', 'a', double('-0.0'), double('0.0')),
  (4, NULL, 'B', NULL, double('0.0')),
  (5, NULL, NULL, NULL, NULL),
  (6, 'x ', 'x', 1.0, 2.0),
  (7, 'İ', 'i̇', 1.0, 2.0),
  (8, 'ς', 'σ', 1.0, 2.0),
  (9, 'K', 'k', 1.0, 2.0),
  (10, 'a\t', 'a ', 1.0, 2.0)

-- Binary strings and floating-point values remain native with the dispatcher enabled.
query expect_native(array_min,array_max)
SELECT id, array_min(array(a, b)), array_max(array(a, b)),
       array_min(array(x, y)), array_max(array(x, y)),
       array_min(array(named_struct('s', a, 'f', x), named_struct('s', b, 'f', y))),
       array_max(array(named_struct('s', a, 'f', x), named_struct('s', b, 'f', y)))
FROM test_array_extrema_collation

-- Case-insensitive ordering differs from binary ordering for 'a' and 'B'.
query expect_native(array_min,array_max)
SELECT id, array_min(array(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE))),
       array_max(array(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_extrema_collation

-- Collation comparison also recurses through struct fields and arrays. The second
-- string field uses binary ordering, independently of the first field's collation.
query expect_native(array_min,array_max)
SELECT id, array_min(array(
         named_struct('s', array(CAST(a AS STRING COLLATE UTF8_LCASE)), 'binary', a, 'f', x),
         named_struct('s', array(CAST(b AS STRING COLLATE UTF8_LCASE)), 'binary', b, 'f', y))),
       array_max(array(
         named_struct('s', array(CAST(a AS STRING COLLATE UTF8_LCASE)), 'binary', a, 'f', x),
         named_struct('s', array(CAST(b AS STRING COLLATE UTF8_LCASE)), 'binary', b, 'f', y)))
FROM test_array_extrema_collation

-- RTRIM ignores only trailing ASCII spaces and keeps the original winning value.
query expect_native(array_min,array_max)
SELECT id,
       array_min(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM))),
       array_max(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM))),
       array_min(array(CAST(a AS STRING COLLATE UTF8_LCASE_RTRIM), CAST(b AS STRING COLLATE UTF8_LCASE_RTRIM))),
       array_max(array(CAST(a AS STRING COLLATE UTF8_LCASE_RTRIM), CAST(b AS STRING COLLATE UTF8_LCASE_RTRIM)))
FROM test_array_extrema_collation

-- ICU locale-sensitive collations continue to use Spark's comparator.
query expect_dispatch(array_min,array_max)
SELECT id, array_min(array(CAST(a AS STRING COLLATE UNICODE_CI), CAST(b AS STRING COLLATE UNICODE_CI))),
       array_max(array(CAST(a AS STRING COLLATE UNICODE_CI), CAST(b AS STRING COLLATE UNICODE_CI)))
FROM test_array_extrema_collation

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
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.expression.ArrayContains.allowIncompatible=false
-- Config: spark.comet.expression.ArraysOverlap.allowIncompatible=false
-- Config: spark.comet.expression.ArrayDistinct.allowIncompatible=false
-- Config: spark.comet.expression.ArrayUnion.allowIncompatible=false

-- array_contains, arrays_overlap, array_distinct and array_union compare array elements for
-- equality. The native kernels compare strings by raw bytes, so under a non-UTF8_BINARY collation
-- they would miss 'a' = 'A' (UTF8_LCASE) or 'x ' = 'x' (UTF8_BINARY_RTRIM). Collated
-- array_contains and arrays_overlap route through the JVM codegen dispatcher, which runs Spark's
-- collation-aware comparison. array_distinct and array_union return arrays, where dispatch costs
-- more than a projection fallback (see ArraySetSupport), so they fall back to Spark instead.

statement
CREATE TABLE test_array_eq_collation(id int, a string, b string) USING parquet

statement
INSERT INTO test_array_eq_collation VALUES
  (1, 'a', 'A'),
  (2, 'x ', 'x'),
  (3, 'b', 'c'),
  (4, NULL, 'a'),
  (5, 'A', NULL)

-- UTF8_BINARY strings stay on the native kernels.
query expect_native(array_contains,arrays_overlap,array_distinct,array_union)
SELECT id, array_contains(array(a), b), arrays_overlap(array(a), array(b)),
       array_distinct(array(a, b)), array_union(array(a), array(b))
FROM test_array_eq_collation

-- UTF8_LCASE: 'a' and 'A' are equal.
query expect_dispatch(array_contains)
SELECT id, array_contains(array(CAST(a AS STRING COLLATE UTF8_LCASE)), CAST(b AS STRING COLLATE UTF8_LCASE))
FROM test_array_eq_collation

query expect_dispatch(arrays_overlap)
SELECT id, arrays_overlap(array(CAST(a AS STRING COLLATE UTF8_LCASE)), array(CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_eq_collation

query expect_fallback(native array_distinct compares raw bytes)
SELECT id, array_distinct(array(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_eq_collation

query expect_fallback(native array_union compares raw bytes)
SELECT id, array_union(array(CAST(a AS STRING COLLATE UTF8_LCASE)), array(CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_eq_collation

-- UTF8_BINARY_RTRIM: 'x ' and 'x' are equal.
query expect_dispatch(array_contains)
SELECT id, array_contains(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM)), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM))
FROM test_array_eq_collation

query expect_dispatch(arrays_overlap)
SELECT id, arrays_overlap(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM)), array(CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM)))
FROM test_array_eq_collation

query expect_fallback(native array_distinct compares raw bytes)
SELECT id, array_distinct(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM)))
FROM test_array_eq_collation

query expect_fallback(native array_union compares raw bytes)
SELECT id, array_union(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM)), array(CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM)))
FROM test_array_eq_collation

-- Collation detection also recurses into struct fields of the array elements.
query expect_dispatch(array_contains,arrays_overlap)
SELECT id,
       array_contains(array(named_struct('s', CAST(a AS STRING COLLATE UTF8_LCASE))),
                      named_struct('s', CAST(b AS STRING COLLATE UTF8_LCASE))),
       arrays_overlap(array(named_struct('s', CAST(a AS STRING COLLATE UTF8_LCASE))),
                      array(named_struct('s', CAST(b AS STRING COLLATE UTF8_LCASE))))
FROM test_array_eq_collation

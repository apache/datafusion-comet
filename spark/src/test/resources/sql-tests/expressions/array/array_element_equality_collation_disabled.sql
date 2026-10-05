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
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.expression.ArrayContains.allowIncompatible=false
-- Config: spark.comet.expression.ArraysOverlap.allowIncompatible=false
-- Config: spark.comet.expression.ArrayDistinct.allowIncompatible=false
-- Config: spark.comet.expression.ArrayUnion.allowIncompatible=false

-- With the JVM codegen dispatcher disabled, collated array_contains and arrays_overlap have no
-- Spark-compatible Comet path and fall back to Spark. array_distinct and array_union fall back
-- whether or not the dispatcher is enabled.

statement
CREATE TABLE test_array_eq_collation(id int, a string, b string) USING parquet

statement
INSERT INTO test_array_eq_collation VALUES (1, 'a', 'A'), (2, 'x ', 'x'), (3, 'b', 'c'), (4, NULL, 'a')

query expect_fallback(array_contains: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, array_contains(array(CAST(a AS STRING COLLATE UTF8_LCASE)), CAST(b AS STRING COLLATE UTF8_LCASE))
FROM test_array_eq_collation

query expect_fallback(arrays_overlap: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, arrays_overlap(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM)), array(CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM)))
FROM test_array_eq_collation

query expect_fallback(native array_distinct compares raw bytes)
SELECT id, array_distinct(array(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_eq_collation

query expect_fallback(native array_union compares raw bytes)
SELECT id, array_union(array(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM)), array(CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM)))
FROM test_array_eq_collation

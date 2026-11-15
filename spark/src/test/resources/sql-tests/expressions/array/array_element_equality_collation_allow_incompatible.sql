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
-- Config: spark.comet.expression.ArrayContains.allowIncompatible=true
-- Config: spark.comet.expression.ArraysOverlap.allowIncompatible=true
-- Config: spark.comet.expression.ArrayDistinct.allowIncompatible=true
-- Config: spark.comet.expression.ArrayUnion.allowIncompatible=true

-- allowIncompatible=true opts collated inputs into the native kernels, which compare raw bytes.
-- The data only holds values whose bytewise and collation-aware equality agree, so the answers
-- still match Spark; the point is that the native path is taken.

statement
CREATE TABLE test_array_eq_collation(id int, a string, b string) USING parquet

statement
INSERT INTO test_array_eq_collation VALUES (1, 'a', 'a'), (2, 'b', 'c'), (3, NULL, 'a')

query expect_native(array_contains,arrays_overlap)
SELECT id,
       array_contains(array(CAST(a AS STRING COLLATE UTF8_LCASE)), CAST(b AS STRING COLLATE UTF8_LCASE)),
       arrays_overlap(array(CAST(a AS STRING COLLATE UTF8_LCASE)), array(CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_eq_collation

query expect_native(array_distinct,array_union)
SELECT id,
       array_distinct(array(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE))),
       array_union(array(CAST(a AS STRING COLLATE UTF8_LCASE)), array(CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_array_eq_collation

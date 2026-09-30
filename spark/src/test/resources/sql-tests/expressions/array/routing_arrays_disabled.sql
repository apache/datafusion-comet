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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false
-- Config: spark.comet.exec.strictFloatingPoint=true
-- Config: spark.comet.expression.ArrayContains.allowIncompatible=false
-- Config: spark.comet.expression.SortArray.allowIncompatible=false
-- Config: spark.comet.expression.ArrayExcept.allowIncompatible=false
-- Config: spark.comet.expression.ArrayJoin.allowIncompatible=false
-- Config: spark.comet.expression.Reverse.allowIncompatible=false

statement
CREATE TABLE routing_arrays(a ARRAY<INT>, f ARRAY<DOUBLE>, x INT, y INT, words ARRAY<STRING>, sep STRING) USING parquet

statement
INSERT INTO routing_arrays VALUES (array(2, 1), array(2.0D, 1.0D), 1, 3, array('a', 'b'), '-'), (array(), array(), 3, 1, array(), ''), (NULL, NULL, NULL, NULL, NULL, NULL)

query expect_native(array_contains)
SELECT array_contains(a, 1) FROM routing_arrays

query expect_native(sort_array)
SELECT sort_array(a) FROM routing_arrays

query expect_native(sequence)
SELECT sequence(x, y) FROM routing_arrays

query expect_native(array_join)
SELECT array_join(words, '-') FROM routing_arrays

query expect_native(reverse)
SELECT reverse(a) FROM routing_arrays

query expect_fallback(array_contains: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT array_contains(f, 1.0D) FROM routing_arrays

query expect_fallback(sort_array: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT sort_array(f) FROM routing_arrays

query expect_fallback(sort_array: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT sort_array(array(make_dt_interval(x), make_dt_interval(y))) FROM routing_arrays

query expect_fallback(array_except: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT sort_array(array_except(a, array(1))) FROM routing_arrays

query expect_fallback(array_except: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT array_except(array(unhex('41')), array(unhex('42')))

query expect_fallback(reverse: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT reverse(array(unhex('41'), unhex('42')))

query expect_fallback(sequence: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT sequence(x, y + 1) FROM routing_arrays

query expect_fallback(sequence: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT sequence(DATE '2024-01-01', DATE '2024-01-03')

query expect_fallback(array_join: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT array_join(words, concat(sep, '')) FROM routing_arrays

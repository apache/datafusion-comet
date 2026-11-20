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

-- array_union with a float element type runs as spark_array_union, which normalizes both arrays
-- before delegating to DataFusion. Like every other element type, it has to skip its right operand
-- where the left array is NULL, as Spark's null-intolerant BinaryExpression does, so a right operand
-- that raises cannot fail a row Spark skips. array_null_short_circuit.sql covers array<int>, which
-- runs through DataFusion's array_union unchanged. Each query names array_union with expect_native,
-- because the codegen dispatcher would pass by running Spark's code.
-- https://github.com/apache/datafusion-comet/issues/6613

-- Float elements run natively by default only on Spark 4.0.5+, 4.1.4+ and 4.2+. The opt-in makes
-- every version run them natively. The data has no signed zero, which earlier releases keep apart.
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.expression.ArrayUnion.allowIncompatible=true

statement
CREATE TABLE array_null_short_circuit_float(a array<double>, b array<double>, s string) USING parquet

-- `s` is malformed only where `a` is NULL. COALESCE(1) writes a single file, so the NULL arrays
-- share a batch with the others.
statement
INSERT INTO array_null_short_circuit_float SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, array(1.5D, 2.5D), 'bad'),
  (array(1.5D, 2.5D), array(2.5D, 3.5D), '2'),
  (array(4.5D), array(5.5D, 6.5D), '7'),
  (NULL, NULL, 'worse'),
  (array(4.5D, NULL, 5.5D), array(5.5D, 6.5D), '5')
  AS v(a, b, s)

-- A start of 0 makes slice raise in any mode, and only rows whose array is NULL have one
query expect_native(array_union)
SELECT array_union(a, slice(b, IF(a IS NULL, 0, 1), 1)) FROM array_null_short_circuit_float

-- The ANSI cast of a malformed string raises, and only rows whose array is NULL have one
query expect_native(array_union)
SELECT array_union(a, array(CAST(CAST(s AS INT) AS DOUBLE))) FROM array_null_short_circuit_float

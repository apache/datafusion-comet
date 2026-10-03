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
-- Config: spark.comet.expression.StringInstr.allowIncompatible=false
-- Config: spark.comet.expression.SubstringIndex.allowIncompatible=false
-- Config: spark.comet.expression.StringTrim.allowIncompatible=false
-- Config: spark.comet.expression.StringTrimLeft.allowIncompatible=false
-- Config: spark.comet.expression.StringTrimRight.allowIncompatible=false
-- Config: spark.comet.expression.Greatest.allowIncompatible=false
-- Config: spark.comet.expression.Least.allowIncompatible=false

-- With the JVM codegen dispatcher disabled, collated instr, substring_index, trim with a trim
-- string, greatest and least have no Spark-compatible Comet path and fall back to Spark.

statement
CREATE TABLE test_string_collation(id int, a string, b string) USING parquet

statement
INSERT INTO test_string_collation VALUES (1, 'a', 'A'), (2, 'x ', 'x'), (3, 'Hello World', 'hello'), (4, NULL, 'a')

query expect_fallback(instr: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, instr(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_fallback(substring_index: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, substring_index(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE), 1)
FROM test_string_collation

query expect_fallback(trim: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, trim(BOTH CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_fallback(ltrim: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, trim(LEADING CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_fallback(rtrim: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, trim(TRAILING CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM) FROM CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM))
FROM test_string_collation

query expect_fallback(greatest: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, greatest(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_fallback(least: spark.comet.exec.scalaUDF.codegen.enabled=false)
SELECT id, least(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM))
FROM test_string_collation

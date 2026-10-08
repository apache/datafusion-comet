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
-- Config: spark.comet.expression.StringInstr.allowIncompatible=true
-- Config: spark.comet.expression.SubstringIndex.allowIncompatible=true
-- Config: spark.comet.expression.StringTrim.allowIncompatible=true
-- Config: spark.comet.expression.StringTrimLeft.allowIncompatible=true
-- Config: spark.comet.expression.StringTrimRight.allowIncompatible=true
-- Config: spark.comet.expression.Greatest.allowIncompatible=true
-- Config: spark.comet.expression.Least.allowIncompatible=true

-- allowIncompatible=true opts collated inputs into the native kernels, which work on raw bytes.
-- The data only holds values whose bytewise and collation-aware answers agree, so the results
-- still match Spark; the point is that the native path is taken.

statement
CREATE TABLE test_string_collation(id int, a string, b string) USING parquet

statement
INSERT INTO test_string_collation VALUES (1, 'abc', 'b'), (2, 'xyz', 'q'), (3, NULL, 'a')

query expect_native(instr,substring_index,trim,ltrim,rtrim,greatest,least)
SELECT id, instr(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE)),
       substring_index(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE), 1),
       trim(BOTH CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE)),
       trim(LEADING CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE)),
       trim(TRAILING CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE)),
       greatest(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE)),
       least(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

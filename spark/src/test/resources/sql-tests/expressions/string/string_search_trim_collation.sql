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
-- Config: spark.comet.expression.StringInstr.allowIncompatible=false
-- Config: spark.comet.expression.SubstringIndex.allowIncompatible=false
-- Config: spark.comet.expression.StringTrim.allowIncompatible=false
-- Config: spark.comet.expression.StringTrimLeft.allowIncompatible=false
-- Config: spark.comet.expression.StringTrimRight.allowIncompatible=false
-- Config: spark.comet.expression.Greatest.allowIncompatible=false
-- Config: spark.comet.expression.Least.allowIncompatible=false

-- instr, substring_index, trim/ltrim/rtrim with a trim string, greatest and least search, trim or
-- order strings. The native kernels do this on raw bytes, so under a non-UTF8_BINARY collation
-- they would miss 'a' = 'A' (UTF8_LCASE) or 'x ' = 'x' (UTF8_BINARY_RTRIM). Collated inputs route
-- through the JVM codegen dispatcher, which runs Spark's collation-aware implementation.

statement
CREATE TABLE test_string_collation(id int, a string, b string) USING parquet

statement
INSERT INTO test_string_collation VALUES
  (1, 'a', 'A'),
  (2, 'x ', 'x'),
  (3, 'b', 'c'),
  (4, 'Hello World', 'hello'),
  (5, 'a,B,c', 'b'),
  (6, 'xX  ', 'X'),
  (7, NULL, 'a'),
  (8, 'A', NULL)

-- UTF8_BINARY strings stay on the native kernels.
query expect_native(instr,substring_index,trim,ltrim,rtrim,greatest,least)
SELECT id, instr(a, b), substring_index(a, b, 1), trim(BOTH b FROM a), trim(LEADING b FROM a),
       trim(TRAILING b FROM a), greatest(a, b), least(a, b)
FROM test_string_collation

-- Without a trim string, trim removes only spaces, which does not depend on the collation, so it
-- stays native even for collated input.
query expect_native(trim,ltrim,rtrim)
SELECT id, trim(CAST(a AS STRING COLLATE UTF8_LCASE)), ltrim(CAST(a AS STRING COLLATE UTF8_LCASE)),
       rtrim(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM))
FROM test_string_collation

-- UTF8_LCASE: 'a' and 'A' are equal.
query expect_dispatch(instr)
SELECT id, instr(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_dispatch(substring_index)
SELECT id, substring_index(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE), 1),
       substring_index(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE), -1)
FROM test_string_collation

query expect_dispatch(trim)
SELECT id, trim(BOTH CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE)),
       btrim(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_dispatch(ltrim)
SELECT id, trim(LEADING CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_dispatch(rtrim)
SELECT id, trim(TRAILING CAST(b AS STRING COLLATE UTF8_LCASE) FROM CAST(a AS STRING COLLATE UTF8_LCASE))
FROM test_string_collation

query expect_dispatch(greatest,least)
SELECT id, greatest(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE)),
       least(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE)),
       greatest(CAST(a AS STRING COLLATE UTF8_LCASE), CAST(b AS STRING COLLATE UTF8_LCASE), 'Z')
FROM test_string_collation

-- UTF8_BINARY_RTRIM: trailing spaces are ignored when comparing.
query expect_dispatch(instr,substring_index)
SELECT id, instr(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM)),
       substring_index(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM), 1)
FROM test_string_collation

query expect_dispatch(trim,ltrim,rtrim)
SELECT id, trim(BOTH CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM) FROM CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM)),
       trim(LEADING CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM) FROM CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM)),
       trim(TRAILING CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM) FROM CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM))
FROM test_string_collation

query expect_dispatch(greatest,least)
SELECT id, greatest(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM)),
       least(CAST(a AS STRING COLLATE UTF8_BINARY_RTRIM), CAST(b AS STRING COLLATE UTF8_BINARY_RTRIM))
FROM test_string_collation

-- A standard ICU collation (UNICODE_CI) also dispatches.
query expect_dispatch(instr,substring_index,trim,greatest,least)
SELECT id, instr(CAST(a AS STRING COLLATE UNICODE_CI), CAST(b AS STRING COLLATE UNICODE_CI)),
       substring_index(CAST(a AS STRING COLLATE UNICODE_CI), CAST(b AS STRING COLLATE UNICODE_CI), 1),
       trim(BOTH CAST(b AS STRING COLLATE UNICODE_CI) FROM CAST(a AS STRING COLLATE UNICODE_CI)),
       greatest(CAST(a AS STRING COLLATE UNICODE_CI), CAST(b AS STRING COLLATE UNICODE_CI)),
       least(CAST(a AS STRING COLLATE UNICODE_CI), CAST(b AS STRING COLLATE UNICODE_CI))
FROM test_string_collation

-- Collation detection also recurses into struct inputs of greatest and least.
query expect_dispatch(greatest,least)
SELECT id, greatest(named_struct('s', CAST(a AS STRING COLLATE UTF8_LCASE)), named_struct('s', CAST(b AS STRING COLLATE UTF8_LCASE))),
       least(named_struct('s', CAST(a AS STRING COLLATE UTF8_LCASE)), named_struct('s', CAST(b AS STRING COLLATE UTF8_LCASE)))
FROM test_string_collation

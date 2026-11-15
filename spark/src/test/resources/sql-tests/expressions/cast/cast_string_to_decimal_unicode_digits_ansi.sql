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

-- cast(string as decimal) with non-ASCII digits, ANSI mode. Spark parses the trimmed string
-- with `new java.math.BigDecimal(s)`, which accepts every Unicode `Nd` digit in the Basic
-- Multilingual Plane, so these must cast to a value rather than raise CAST_INVALID_INPUT.
-- Supplementary-plane digits are rejected by BigDecimal and must still raise.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_cast_str_dec_unicode_ansi(id int, s string) USING parquet

-- Arabic-Indic, Devanagari, Thai, fullwidth, signs, '.', rounding, exponent digits, mixed
-- scripts and ASCII trim; every value fits decimal(10,2)
statement
INSERT INTO test_cast_str_dec_unicode_ansi VALUES
  (1, '٣'),
  (2, '३'),
  (3, '๓'),
  (4, '１２３.４５'),
  (5, '-٣'),
  (6, '+٣.٣٣'),
  (7, '١٢٣.٤٥٥'),
  (8, '1e٣'),
  (9, '1E-٣'),
  (10, '1٢e३'),
  (11, concat(' ٣', chr(9))),
  (12, NULL)

statement
CREATE TABLE test_cast_str_dec_unicode_ansi_bad(s string) USING parquet

-- U+1D7D0 MATHEMATICAL BOLD DIGIT TWO is Nd but outside the BMP
statement
INSERT INTO test_cast_str_dec_unicode_ansi_bad VALUES ('𝟐')

statement
CREATE TABLE test_cast_str_dec_unicode_ansi_big(s string) USING parquet

statement
INSERT INTO test_cast_str_dec_unicode_ansi_big VALUES ('٣٣٣٣٣٣٣٣٣')

query
SELECT id, cast(s as decimal(10,2)), cast(s as decimal(38,10))
FROM test_cast_str_dec_unicode_ansi ORDER BY id

query
SELECT id, try_cast(s as decimal(10,2)) FROM test_cast_str_dec_unicode_ansi ORDER BY id

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s as decimal(10,2)) FROM test_cast_str_dec_unicode_ansi_bad

-- the value is valid but does not fit decimal(10,2)
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT cast(s as decimal(10,2)) FROM test_cast_str_dec_unicode_ansi_big

-- try_cast suppresses both errors
query
SELECT try_cast(s as decimal(10,2)) FROM test_cast_str_dec_unicode_ansi_bad
UNION ALL
SELECT try_cast(s as decimal(10,2)) FROM test_cast_str_dec_unicode_ansi_big

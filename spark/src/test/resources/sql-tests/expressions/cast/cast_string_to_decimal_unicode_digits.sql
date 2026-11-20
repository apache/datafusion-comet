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

-- cast(string as decimal) with non-ASCII digits, non-ANSI mode. Spark parses the trimmed string
-- with `new java.math.BigDecimal(s)`, which accepts every char for which
-- `Character.isDigit(char)` is true: all Unicode `Nd` digits in the Basic Multilingual Plane,
-- in the mantissa and in the exponent, mixed freely across scripts. Supplementary-plane digits
-- (e.g. U+1D7D0 MATHEMATICAL BOLD DIGIT TWO) are surrogate pairs and are rejected.
--
-- The integral, floating-point and date casts of the same strings are ASCII-only in Spark
-- (UTF8String.toLong, Double.parseDouble, SparkDateTimeUtils.stringToDate) and must stay NULL.

-- Config: spark.sql.ansi.enabled=false

statement
CREATE TABLE test_cast_str_dec_unicode(id int, s string) USING parquet

-- 1: U+0663 ARABIC-INDIC DIGIT THREE
-- 2: U+06F3 EXTENDED ARABIC-INDIC DIGIT THREE
-- 3: U+0969 DEVANAGARI DIGIT THREE
-- 4: U+09E9 BENGALI DIGIT THREE
-- 5: U+0E53 THAI DIGIT THREE
-- 6: U+07C3 NKO DIGIT THREE
-- 7: U+A9D3 JAVANESE DIGIT THREE
-- 8: fullwidth digits
-- 9-17: signs, '.', HALF_UP rounding, exponent digits, mixed scripts, ASCII trim
-- 18: 20 Arabic-Indic digits, too many for the cast targets below
-- 19-25: rejected by BigDecimal: supplementary-plane Nd digits, ROMAN NUMERAL FOUR (Nl),
--        SUPERSCRIPT TWO (No), ARABIC DECIMAL SEPARATOR, leading NBSP (not trimmed)
statement
INSERT INTO test_cast_str_dec_unicode VALUES
  (1, '٣'),
  (2, '۳'),
  (3, '३'),
  (4, '৩'),
  (5, '๓'),
  (6, '߃'),
  (7, '꧓'),
  (8, '１２３.４５'),
  (9, '1٣'),
  (10, '-٣'),
  (11, '+٣.٣٣'),
  (12, '.٣'),
  (13, '١٢٣.٤٥٥'),
  (14, '1e٣'),
  (15, '1E-٣'),
  (16, '1٢e३'),
  (17, concat(' ٣', chr(9))),
  (18, '٣٣٣٣٣٣٣٣٣٣٣٣٣٣٣٣٣٣٣٣'),
  (19, '𝟐'),
  (20, '1𝟐'),
  (21, '1e𝟐'),
  (22, 'Ⅳ'),
  (23, '²'),
  (24, '١٫٥'),
  (25, concat(chr(160), '٣')),
  (26, NULL)

query
SELECT id, cast(s as decimal(10,2)), cast(s as decimal(38,10)), cast(s as decimal(5,0))
FROM test_cast_str_dec_unicode ORDER BY id

query
SELECT id, try_cast(s as decimal(10,2)), try_cast(s as decimal(38,0))
FROM test_cast_str_dec_unicode ORDER BY id

-- Spark's integral, floating-point and date parsers only accept ASCII digits
query
SELECT id, cast(s as int), cast(s as bigint), cast(s as double), cast(s as date)
FROM test_cast_str_dec_unicode ORDER BY id

-- literal arguments
query
SELECT cast('٣' as decimal(10,2)), cast('१२३.४५' as decimal(10,2)), cast('𝟐' as decimal(10,2))

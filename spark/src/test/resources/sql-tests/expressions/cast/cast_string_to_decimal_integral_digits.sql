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

-- cast(string as decimal) of a string whose integral part has more than 38 digits, non-ANSI mode.
-- Spark rejects such a string as soon as it has parsed it, before it checks the value against the
-- target type, so the result is NULL whatever the target type. With
-- spark.sql.legacy.allowNegativeScaleOfDecimal Spark skips the limit: a string that does not fit
-- the target type is NULL for that reason, and a zero with a large exponent is zero.
-- cast_string_to_decimal_integral_digits_ansi.sql has the errors of ANSI mode.
-- https://github.com/apache/datafusion-comet/issues/7188

-- Config: spark.sql.ansi.enabled=false

statement
CREATE TABLE test_cast_str_dec_digits(id int, s string) USING parquet

-- ids 1 to 7 have more than 38 integral digits: an exponent that adds them, 39 digits that fit an
-- i128 and 40 that do not, a fraction after 40 digits, and leading zeros that do not count.
-- ids 10 to 16 are within the limit, up to 38 digits.
-- ids 20 to 23 are zeros with a large exponent. Without the setting the result depends on whether
-- Spark has SPARK-60118, so they are only queried after it.
statement
INSERT INTO test_cast_str_dec_digits VALUES
  (1, '6E+38'),
  (2, '100000000000000000000000000000000000000'),
  (3, '1000000000000000000000000000000000000000'),
  (4, '-1.5e50'),
  (5, '9999999999999999999999999999999999999999.5'),
  (6, '0.1e40'),
  (7, '000000000000000000000000000000000000000000000000001e38'),
  (10, '12345678901'),
  (11, '99999999999999999999999999999999999999'),
  (12, '1.5'),
  (13, '-12'),
  (14, '1e-50'),
  (15, NULL),
  (16, '10000000000000000000000000000000000000'),
  (20, '0e38'),
  (21, '0E+73'),
  (22, '+0e83'),
  (23, '0e5887677')

-- the target type does not change the result, even one that is as wide as a decimal can be
query
SELECT id, cast(s as decimal(10,2)), cast(s as decimal(38,10)), cast(s as decimal(38,0)),
  cast(s as decimal(38,38))
FROM test_cast_str_dec_digits WHERE id < 20 ORDER BY id

-- With the setting Spark skips the limit, so a string with too many integral digits is an
-- ordinary value that is too large for the target type. A zero with a large exponent is zero.
statement
SET spark.sql.legacy.allowNegativeScaleOfDecimal=true

query
SELECT id, cast(s as decimal(10,2)), cast(s as decimal(38,10)), cast(s as decimal(38,0))
FROM test_cast_str_dec_digits ORDER BY id

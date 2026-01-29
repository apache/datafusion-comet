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

-- Config: spark.comet.expression.Cast.allowIncompatible=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- Decimal arithmetic exercises CheckOverflow, MakeDecimal, UnscaledValue
statement
CREATE TABLE test_decimal(a decimal(10,2), b decimal(10,2)) USING parquet

statement
INSERT INTO test_decimal VALUES (10.50, 3.20), (0.00, 1.00), (-10.50, 3.20), (99999999.99, 0.01), (NULL, 1.00)

query
SELECT a + b, a - b, a * b, a / b FROM test_decimal

query
SELECT a % b FROM test_decimal

-- Mixed precision
statement
CREATE TABLE test_decimal_mix(a decimal(18,6), b decimal(10,2)) USING parquet

statement
INSERT INTO test_decimal_mix VALUES (123456.789012, 99.99), (0.000001, 1.00), (-123456.789012, 0.01), (NULL, NULL)

query
SELECT a + b, a - b, a * b FROM test_decimal_mix

query spark_answer_only
SELECT a / b FROM test_decimal_mix

-- Cast to decimal
statement
CREATE TABLE test_dec_cast(i int, l long, d double, s string) USING parquet

statement
INSERT INTO test_dec_cast VALUES (42, 123456789, 3.14159, '99.99'), (0, 0, 0.0, '0.00'), (NULL, NULL, NULL, NULL)

query
SELECT cast(i as decimal(10,2)), cast(l as decimal(18,2)), cast(d as decimal(10,5)), cast(s as decimal(10,2)) FROM test_dec_cast

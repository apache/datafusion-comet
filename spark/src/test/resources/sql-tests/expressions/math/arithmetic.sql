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

-- ConfigMatrix: parquet.enable.dictionary=false,true

-- negative
statement
CREATE TABLE test_neg(col1 int) USING parquet

statement
INSERT INTO test_neg VALUES(1), (2), (3), (3)

query
SELECT negative(col1), -(col1) FROM test_neg

-- integral division overflow
statement
CREATE TABLE test_div(c1 long, c2 short) USING parquet

statement
INSERT INTO test_div VALUES(-9223372036854775808, -1)

query
SELECT c1 div c2 FROM test_div ORDER BY c1

-- Add, Subtract, Multiply, Divide, Remainder
statement
CREATE TABLE test_arith(a int, b int, la long, lb long, fa float, fb float, da double, db double) USING parquet

statement
INSERT INTO test_arith VALUES (10, 3, 10, 3, 10.5, 3.2, 10.5, 3.2), (0, 1, 0, 1, 0.0, 1.0, 0.0, 1.0), (-10, 3, -10, 3, -10.5, 3.2, -10.5, 3.2), (2147483647, 1, 9223372036854775807, 1, cast('Infinity' as float), 1.0, cast('NaN' as double), 1.0), (NULL, 1, NULL, 1, NULL, 1.0, NULL, 1.0)

query
SELECT a + b, a - b, a * b, a / b, a % b FROM test_arith

query
SELECT la + lb, la - lb, la * lb, la / lb, la % lb FROM test_arith

query
SELECT fa + fb, fa - fb, fa * fb, fa / fb FROM test_arith

query
SELECT da + db, da - db, da * db, da / db FROM test_arith

-- division by zero
statement
CREATE TABLE test_div_zero(a int, b int, d double) USING parquet

statement
INSERT INTO test_div_zero VALUES (1, 0, 0.0), (0, 0, 0.0)

query
SELECT a / b, a % b, d / 0.0 FROM test_div_zero

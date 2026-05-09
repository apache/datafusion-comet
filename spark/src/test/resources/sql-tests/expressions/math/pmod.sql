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

statement
CREATE TABLE test_pmod(a int, b int, la long, lb long, fa float, fb float, da double, db double) USING parquet

statement
INSERT INTO test_pmod VALUES (10, 3, 10, 3, 10.5, 3.0, 10.5, 3.0), (-10, 3, -10, 3, -10.5, 3.0, -10.5, 3.0), (10, -3, 10, -3, 10.5, -3.0, 10.5, -3.0), (-10, -3, -10, -3, -10.5, -3.0, -10.5, -3.0), (0, 3, 0, 3, 0.0, 3.0, 0.0, 3.0), (NULL, 3, NULL, 3, NULL, 3.0, NULL, 3.0), (10, NULL, 10, NULL, 10.5, cast(NULL as float), 10.5, cast(NULL as double))

-- column arguments: int
query
SELECT pmod(a, b) FROM test_pmod

-- column arguments: long
query
SELECT pmod(la, lb) FROM test_pmod

-- column arguments: float
query
SELECT pmod(fa, fb) FROM test_pmod

-- column arguments: double
query
SELECT pmod(da, db) FROM test_pmod

-- literal arguments
query
SELECT pmod(10, 3), pmod(-10, 3), pmod(10, -3), pmod(-10, -3)

-- literal with NULL
query
SELECT pmod(NULL, 3), pmod(10, NULL), pmod(NULL, NULL)

-- division by zero returns NULL in non-ANSI mode
query
SELECT pmod(10, 0), pmod(-10, 0)

-- decimal type
statement
CREATE TABLE test_pmod_dec(a decimal(10,2), b decimal(10,2)) USING parquet

statement
INSERT INTO test_pmod_dec VALUES (10.50, 3.00), (-10.50, 3.00), (10.50, -3.00), (-10.50, -3.00), (NULL, 3.00)

query
SELECT pmod(a, b) FROM test_pmod_dec

-- short type
statement
CREATE TABLE test_pmod_short(a short, b short) USING parquet

statement
INSERT INTO test_pmod_short VALUES (10, 3), (-10, 3), (10, -3), (-10, -3)

query
SELECT pmod(a, b) FROM test_pmod_short

-- byte type
statement
CREATE TABLE test_pmod_byte(a byte, b byte) USING parquet

statement
INSERT INTO test_pmod_byte VALUES (10, 3), (-10, 3), (10, -3), (-10, -3)

query
SELECT pmod(a, b) FROM test_pmod_byte

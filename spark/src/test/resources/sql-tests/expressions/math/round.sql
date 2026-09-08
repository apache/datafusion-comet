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

-- Integral and non-negative-scale decimal inputs round natively. Float and double inputs have no
-- native implementation (Spark rounds them through BigDecimal built from Double.toString), so they
-- route through the codegen dispatcher and must match Spark exactly.

statement
CREATE TABLE test_round(d double, f float, dec decimal(10,4), i int, l bigint) USING parquet

statement
INSERT INTO test_round VALUES
 (2.5, 2.5, 2.5, 25, 25),
 (3.5, 3.5, 3.5, 35, 35),
 (-2.5, -2.5, -2.5, -25, -25),
 (123.456, 123.456, 123.456, 123, 123),
 (0.0, 0.0, 0.0, 0, 0),
 (-0.0D, -0.0F, 0.0, 0, 0),
 (NULL, NULL, NULL, NULL, NULL),
 (cast('NaN' as double), cast('NaN' as float), 0.0, 0, 0),
 (cast('Infinity' as double), cast('Infinity' as float), 0.0, 0, 0),
 (cast('-Infinity' as double), cast('-Infinity' as float), 0.0, 0, 0)

query
SELECT d, round(d), round(d, 0), round(d, 2), round(d, -1) FROM test_round

query
SELECT f, round(f), round(f, 0), round(f, 2), round(f, -1) FROM test_round

-- Null scale makes the whole result null without evaluating the child.
query
SELECT round(d, NULL), round(f, NULL) FROM test_round

-- Decimal and integral inputs stay on the native path.
query
SELECT dec, round(dec), round(dec, 2), round(dec, -1) FROM test_round

query
SELECT i, round(i, 0), round(i, -1), round(l, -1) FROM test_round

-- Doubles whose shortest decimal representation rounds differently than the exact binary value.
-- -5.81855622136895E8 is exactly -581855622.13689494132995605468750, but Double.toString gives
-- -5.81855622136895E8, so Spark rounds the 5th fractional digit up. 6.1317116247283497E18 is
-- exactly 6131711624728349696, which Double.toString does not round up. Both cases are why there
-- is no native float/double path.
statement
CREATE TABLE test_round_repr(d double) USING parquet

statement
INSERT INTO test_round_repr VALUES (-5.81855622136895E8), (6.1317116247283497E18)

query
SELECT d, round(d, 5), round(d, -5) FROM test_round_repr

-- literal + literal
query
SELECT round(123.456, 2), round(2.5, 0), round(3.5, 0), round(-2.5, 0), round(NULL, 0)

query
SELECT round(2.5D, 0), round(3.5D, 0), round(-2.5D, 0), round(2.5F, 0), round(-2.5F, 0)

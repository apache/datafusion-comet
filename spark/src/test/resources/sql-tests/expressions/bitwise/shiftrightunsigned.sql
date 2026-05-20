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

-- Spark's ShiftRightUnsigned: first arg is Int or Long, second is Int.
-- Returns the same integer type as the first argument. Shift amount is
-- normalized to the bit width (Java semantics) for negative/large shifts.

statement
CREATE TABLE test_shiftrightunsigned_int(v int, s int) USING parquet

statement
INSERT INTO test_shiftrightunsigned_int VALUES
  (1, 1),
  (-1, 1),
  (8, 2),
  (2147483647, 1),
  (-2147483648, 1),
  (0, 0),
  (1, 0),
  (1, 31),
  (1, 32),
  (1, 33),
  (1, -1),
  (NULL, 1),
  (1, NULL)

query
SELECT shiftrightunsigned(v, s) FROM test_shiftrightunsigned_int

statement
CREATE TABLE test_shiftrightunsigned_long(v bigint, s int) USING parquet

statement
INSERT INTO test_shiftrightunsigned_long VALUES
  (1, 1),
  (-1, 1),
  (9223372036854775807, 1),
  (-9223372036854775808, 1),
  (0, 0),
  (1, 63),
  (1, 64),
  (1, -1),
  (NULL, 1),
  (1, NULL)

query
SELECT shiftrightunsigned(v, s) FROM test_shiftrightunsigned_long

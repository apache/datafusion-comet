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

-- BIGINT values with INT amounts exercise Comet's cast of the shift amount.
-- Migrated from CometBitwiseExpressionSuite's mixed-type shift test (#6634).
statement
CREATE TABLE test_shift_mixed_types(v bigint, amount int) USING parquet

statement
INSERT INTO test_shift_mixed_types
SELECT /*+ COALESCE(1) */ v, amount
FROM VALUES
  (1111, 2),
  (1111, 2),
  (3333, 4),
  (5555, 6),
  (4294967296, 1),
  (-4294967296, 1),
  (9223372036854775807, 1),
  (-9223372036854775808, 1),
  (0, 0),
  (NULL, 2),
  (1111, NULL)
AS input(v, amount)

query
SELECT shiftright(v, 2), shiftright(v, amount) FROM test_shift_mixed_types

query
SELECT shiftleft(v, 2), shiftleft(v, amount) FROM test_shift_mixed_types

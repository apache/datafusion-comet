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

-- Tests for array_insert with legacy negative index mode enabled.
-- In legacy mode, -1 inserts BEFORE the last element (not after).

-- ConfigMatrix: parquet.enable.dictionary=false,true
-- Config: spark.comet.expression.ArrayInsert.allowIncompatible=true
-- Config: spark.sql.legacy.negativeIndexInArrayInsert=true

-- -1 inserts before last element in legacy mode
query
SELECT array_insert(array(1, 2, 3), -1, 10)

-- -2 inserts before second-to-last
query
SELECT array_insert(array(1, 2, 3), -2, 10)

-- -3 inserts before first element
query
SELECT array_insert(array(1, 2, 3), -3, 10)

-- negative beyond start with null padding (legacy mode pads differently)
query
SELECT array_insert(array(1, 2, 3), -5, 10)

-- far negative beyond start
query
SELECT array_insert(array(1, 3, 4), -2, 2)

-- column-based test
statement
CREATE TABLE test_ai_legacy(arr array<int>, pos int, val int) USING parquet

statement
INSERT INTO test_ai_legacy VALUES
  (array(1, 2, 3), -1, 10),
  (array(4, 5), -1, 20),
  (array(1, 2, 3), -4, 10),
  (NULL, -1, 10)

query
SELECT array_insert(arr, pos, val) FROM test_ai_legacy

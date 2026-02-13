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

-- ANSI mode array index access tests
-- Tests that array[index] throws exceptions for out-of-bounds access in ANSI mode

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Test data setup
-- ============================================================================

statement
CREATE TABLE ansi_array_oob(arr array<int>) USING parquet

statement
INSERT INTO ansi_array_oob VALUES (array(1, 2, 3))

-- ============================================================================
-- Array index out of bounds (positive index)
-- Spark throws: [INVALID_ARRAY_INDEX] The index X is out of bounds
-- Comet throws: Index out of bounds for array
-- See https://github.com/apache/datafusion-comet/issues/3375
-- ============================================================================

-- index beyond array length should throw (0-based indexing)
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT arr[10] FROM ansi_array_oob

-- literal array with out of bounds access
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT array(1, 2, 3)[5]

-- ============================================================================
-- Array index out of bounds (negative index)
-- ============================================================================

-- negative index should throw
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT arr[-1] FROM ansi_array_oob

-- literal with negative index
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT array(1, 2, 3)[-1]

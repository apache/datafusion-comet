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
CREATE TABLE ansi_array_oob(arr array<int>, positive_idx int, negative_idx int) USING parquet

statement
INSERT INTO ansi_array_oob VALUES (array(1, 2, 3), 5, -1)

-- Valid boundary indices must run natively as well as match Spark.
query
SELECT arr[0], arr[2] FROM ansi_array_oob

-- ============================================================================
-- Array index out of bounds (positive index)
-- Spark and Comet throw INVALID_ARRAY_INDEX in ANSI mode.
-- ============================================================================

-- index beyond array length should throw (0-based indexing)
query expect_error([INVALID_ARRAY_INDEX])
SELECT arr[3] FROM ansi_array_oob

query expect_error([INVALID_ARRAY_INDEX])
SELECT arr[10] FROM ansi_array_oob

-- Use a column index so SimplifyExtractValueOps cannot replace the lookup with NULL.
query expect_error([INVALID_ARRAY_INDEX])
SELECT array(1, 2, 3)[positive_idx] FROM ansi_array_oob

-- ============================================================================
-- Array index out of bounds (negative index)
-- ============================================================================

-- negative index should throw
query expect_error([INVALID_ARRAY_INDEX])
SELECT arr[-1] FROM ansi_array_oob

-- literal array with a negative column index
query expect_error([INVALID_ARRAY_INDEX])
SELECT array(1, 2, 3)[negative_idx] FROM ansi_array_oob

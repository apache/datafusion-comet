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

-- ANSI mode element_at tests
-- Tests that element_at throws exceptions for out-of-bounds access in ANSI mode
-- Note: element_at uses 1-based indexing

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Test data setup
-- ============================================================================

statement
CREATE TABLE ansi_element_at_oob(arr array<int>) USING parquet

statement
INSERT INTO ansi_element_at_oob VALUES (array(1, 2, 3))

-- ============================================================================
-- element_at index out of bounds (positive index)
-- Spark throws: [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] ...
-- Comet throws: Index out of bounds for array
-- See https://github.com/apache/datafusion-comet/issues/3375
-- ============================================================================

-- index beyond array length should throw (1-based indexing)
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT element_at(arr, 10) FROM ansi_element_at_oob

-- literal array with out of bounds access
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT element_at(array(1, 2, 3), 5)

-- ============================================================================
-- element_at with index 0 (invalid)
-- Spark throws: [INVALID_INDEX_OF_ZERO] The index 0 is invalid
-- Comet throws: different error message
-- See https://github.com/apache/datafusion-comet/issues/3375
-- ============================================================================

-- index 0 is not valid for element_at (1-based indexing)
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT element_at(arr, 0) FROM ansi_element_at_oob

-- literal with index 0
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT element_at(array(1, 2, 3), 0)

-- ============================================================================
-- element_at index out of bounds (negative index beyond array)
-- ============================================================================

-- negative index beyond array size should throw
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT element_at(arr, -10) FROM ansi_element_at_oob

-- literal with negative out of bounds
query ignore(https://github.com/apache/datafusion-comet/issues/3375)
SELECT element_at(array(1, 2, 3), -5)

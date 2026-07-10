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

-- MinSparkVersion: 4.0

-- Spark 4.0+ widens SortArray.ascendingOrder from a boolean Literal to any foldable boolean
-- expression. Spark 3.x rejects a non-Literal ascendingOrder at analysis, so this file is gated
-- to 4.0+. CometSqlFileTestSuite disables ConstantFolding, so the foldable casts below reach the
-- serde unfolded (as Cast nodes, not Literals) and exercise the convert-time evaluation path.

statement
CREATE TABLE test_sort_array_foldable(arr array<int>) USING parquet

statement
INSERT INTO test_sort_array_foldable VALUES
  (array(3, 1, 4, 1, 5)),
  (array(3, NULL, 1, NULL, 2)),
  (array()),
  (NULL)

-- foldable ascending (cast(1 as boolean) => true)
query
SELECT sort_array(arr, cast(1 as boolean)) FROM test_sort_array_foldable

-- foldable descending (cast(0 as boolean) => false)
query
SELECT sort_array(arr, cast(0 as boolean)) FROM test_sort_array_foldable

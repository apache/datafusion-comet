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

-- Spark 3.4/3.5's SortArray requires the ascendingOrder argument to be a boolean literal and
-- rejects any other expression at analysis time. Spark 4.0 relaxed this to accept any foldable
-- boolean, so the non-literal-ascendingOrder cases below can only be exercised on Spark 4.0+.
-- MinSparkVersion: 4.0

statement
CREATE TABLE test_sort_array_int(arr array<int>) USING parquet

statement
INSERT INTO test_sort_array_int VALUES
  (array(3, 1, 4, 1, 5)),
  (array(3, NULL, 1, NULL, 2)),
  (array()),
  (NULL)

-- ascendingOrder given as a foldable non-literal expression. Spark requires ascendingOrder to be
-- foldable, but this suite disables constant folding, so the serde sees `And`/`Or` rather than a
-- boolean literal. That has no native path, but SortArray mixes in CodegenDispatchFallback, so it
-- routes through the JVM codegen dispatcher (Spark's own SortArray.doGenCode inside the Comet
-- pipeline) and stays native while matching Spark exactly.
query
SELECT sort_array(arr, true AND true) FROM test_sort_array_int

query
SELECT sort_array(arr, false OR false) FROM test_sort_array_int

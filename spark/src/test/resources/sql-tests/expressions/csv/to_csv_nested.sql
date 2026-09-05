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

-- Full-value coverage of `to_csv` over complex field types, which `to_csv.sql` can only assert for
-- non-nullness because Spark 3.4 / 3.5 render a complex field as a Java identity string. Spark 4.0
-- gave `UnivocityGenerator.makeConverter` real array / map / struct branches ("[1, 2, 3]",
-- "{k -> 10}", "{5, z}"), built from the typed `getArray` / `getMap` / `getStruct` getters, so from
-- 4.0 on the value is deterministic and Comet's kernel-side `CometInternalRow` / `CometArrayData` /
-- `CometMapData` getters have to agree with Spark's row-side ones element for element.
-- MinSparkVersion: 4.0

statement
CREATE TABLE test_to_csv_nested4(s struct<i: int, arr: array<int>, m: map<string, int>, n: struct<x: int, y: string>>) USING parquet

statement
INSERT INTO test_to_csv_nested4 VALUES
  (named_struct('i', 1, 'arr', array(1, 2, 3), 'm', map('k', 10), 'n', named_struct('x', 5, 'y', 'z'))),
  (named_struct('i', 2, 'arr', array(), 'm', map(), 'n', named_struct('x', NULL, 'y', NULL))),
  (named_struct('i', 3, 'arr', array(1, CAST(NULL AS int)), 'm', map('k', CAST(NULL AS int)), 'n', named_struct('x', 7, 'y', 'a,b'))),
  (named_struct('i', NULL, 'arr', CAST(NULL AS array<int>), 'm', CAST(NULL AS map<string, int>), 'n', CAST(NULL AS struct<x: int, y: string>))),
  (CAST(NULL AS struct<i: int, arr: array<int>, m: map<string, int>, n: struct<x: int, y: string>>))

-- Struct column straight from the scan: the converter reads the array / map / struct fields off
-- the kernel's CometInternalRow. Empty collections, null elements and a value needing CSV quoting
-- are all in the data above.
query
SELECT to_csv(s) FROM test_to_csv_nested4

-- nullValue changes how a null *element* inside a complex field renders (`appendNull` only emits
-- when the option was set explicitly), which is a separate code path from a null top-level field.
query
SELECT to_csv(s, map('nullValue', 'NIL')) FROM test_to_csv_nested4

-- named_struct over the complex fields, so the values pass through CreateNamedStruct's row before
-- the converter reads them back.
query
SELECT to_csv(named_struct('arr', s.arr, 'm', s.m, 'n', s.n)) FROM test_to_csv_nested4

-- Complex inside complex: the converter recurses, so an array-of-array read goes through the
-- kernel's element getters rather than stopping at the first level.
query
SELECT to_csv(named_struct('aa', array(array(1, 2), array(3)), 'ms', map('k', named_struct('x', 1)))) FROM test_to_csv_nested4

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

-- For a struct or array key type, `ArrayBasedMapBuilder` finds duplicate keys with
-- `TypeUtils.getInterpretedOrdering` on every Spark version, and that ordering treats `-0.0` and
-- `+0.0` (and all `NaN`s) as equal at any nesting level. Comet's native builders compare the
-- nested values by their bits and would keep both keys, so both constructors decline a key type
-- that contains a floating-point field. `CometMapFromArrays` has no codegen dispatcher, so the
-- projection falls back to Spark; `CometMapFromEntries` mixes in `CodegenDispatchFallback`, so it
-- stays in the Comet pipeline running Spark's own generated code. This file runs under the default
-- `EXCEPTION` policy; `map_builders_nested_fp_last_win.sql` covers `LAST_WIN`.

statement
CREATE TABLE test_map_builders_nested_fp(
  id int,
  ks array<struct<a: double>>,
  ka array<array<float>>,
  es array<struct<key: struct<a: double>, value: int>>,
  ea array<struct<key: array<float>, value: int>>) USING parquet

statement
INSERT INTO test_map_builders_nested_fp VALUES
  (1,
   array(named_struct('a', double('-0.0')), named_struct('a', 0.0D)),
   array(array(float('-0.0')), array(float('0.0'))),
   array(named_struct('key', named_struct('a', double('-0.0')), 'value', 1),
         named_struct('key', named_struct('a', 0.0D), 'value', 2)),
   array(named_struct('key', array(float('-0.0')), 'value', 1),
         named_struct('key', array(float('0.0')), 'value', 2))),
  (2,
   array(named_struct('a', 1.0D), named_struct('a', 2.0D)),
   array(array(float('1.0')), array(float('2.0'))),
   array(named_struct('key', named_struct('a', 1.0D), 'value', 1),
         named_struct('key', named_struct('a', 2.0D), 'value', 2)),
   array(named_struct('key', array(float('1.0')), 'value', 1),
         named_struct('key', array(float('2.0')), 'value', 2)))

-- Keys that do not repeat: both engines return the maps, so these queries pin where each
-- constructor ran.
query expect_fallback(duplicate struct or array keys)
SELECT map_from_arrays(ks, array(1, 2)), map_from_arrays(ka, array(1, 2))
FROM test_map_builders_nested_fp WHERE id = 2

query expect_dispatch(map_from_entries)
SELECT map_from_entries(es), map_from_entries(ea) FROM test_map_builders_nested_fp WHERE id = 2

-- `-0.0` and `+0.0` nested in a key are one key to Spark.
query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(ks, array(1, 2)) FROM test_map_builders_nested_fp WHERE id = 1

query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(ka, array(1, 2)) FROM test_map_builders_nested_fp WHERE id = 1

query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_entries(es) FROM test_map_builders_nested_fp WHERE id = 1

query expect_error(DUPLICATED_MAP_KEY)
SELECT map_from_entries(ea) FROM test_map_builders_nested_fp WHERE id = 1

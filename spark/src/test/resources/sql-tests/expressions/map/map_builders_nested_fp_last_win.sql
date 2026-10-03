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

-- Under `LAST_WIN`, Spark keeps one entry for keys that differ only by `-0.0` and `+0.0` nested in
-- a struct or array: the first key's slot with the last value. Both constructors decline these key
-- types, so they return Spark's answer; see `map_builders_nested_fp.sql` for why and for the
-- default `EXCEPTION` policy.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

statement
CREATE TABLE test_map_builders_nested_fp_last_win(
  ks array<struct<a: double>>,
  ka array<array<float>>,
  es array<struct<key: struct<a: double>, value: int>>,
  ea array<struct<key: array<float>, value: int>>) USING parquet

statement
INSERT INTO test_map_builders_nested_fp_last_win VALUES
  (array(named_struct('a', double('-0.0')), named_struct('a', 0.0D)),
   array(array(float('-0.0')), array(float('0.0'))),
   array(named_struct('key', named_struct('a', double('-0.0')), 'value', 1),
         named_struct('key', named_struct('a', 0.0D), 'value', 2)),
   array(named_struct('key', array(float('-0.0')), 'value', 1),
         named_struct('key', array(float('0.0')), 'value', 2))),
  (array(named_struct('a', 1.0D), named_struct('a', 2.0D)),
   array(array(float('1.0')), array(float('2.0'))),
   array(named_struct('key', named_struct('a', 1.0D), 'value', 1),
         named_struct('key', named_struct('a', 2.0D), 'value', 2)),
   array(named_struct('key', array(float('1.0')), 'value', 1),
         named_struct('key', array(float('2.0')), 'value', 2)))

-- The harness collects a map into a Scala `Map`, where `-0.0` and `0.0` are equal keys, so a map
-- that kept both keys would still compare equal. `map_keys` and `map_values` pin the entries.
query expect_fallback(duplicate struct or array keys)
SELECT map_keys(map_from_arrays(ks, array(1, 2))), map_values(map_from_arrays(ks, array(1, 2))),
       map_keys(map_from_arrays(ka, array(1, 2))), map_values(map_from_arrays(ka, array(1, 2)))
FROM test_map_builders_nested_fp_last_win

query expect_dispatch(map_from_entries)
SELECT map_keys(map_from_entries(es)), map_values(map_from_entries(es)),
       map_keys(map_from_entries(ea)), map_values(map_from_entries(ea))
FROM test_map_builders_nested_fp_last_win

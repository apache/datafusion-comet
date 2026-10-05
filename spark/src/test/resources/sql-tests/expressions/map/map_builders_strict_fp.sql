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

-- The native map constructors compare and store a floating-point key as Spark does on every
-- version (see the map_funcs expression audit), so they stay native with
-- `spark.comet.exec.strictFloatingPoint` on, for a floating-point key type and a floating-point
-- value type alike. A struct or array key that holds a floating-point field still declines; see
-- `map_builders_nested_fp.sql`.

-- Config: spark.comet.exec.strictFloatingPoint=true

statement
CREATE TABLE test_map_builders_strict_fp(k double, v int) USING parquet

statement
INSERT INTO test_map_builders_strict_fp VALUES (1.0D, 1), (double('-0.0'), 2), (2.5D, 3)

query expect_native(map_from_arrays)
SELECT map_from_arrays(array(k), array(v)) FROM test_map_builders_strict_fp

query expect_native(map_from_entries)
SELECT map_from_entries(array(struct(k, v))) FROM test_map_builders_strict_fp

query expect_native(map_from_arrays)
SELECT map_from_arrays(array(v), array(k)) FROM test_map_builders_strict_fp

query expect_native(map_from_entries)
SELECT map_from_entries(array(struct(v, k))) FROM test_map_builders_strict_fp

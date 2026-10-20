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

-- With `spark.comet.exec.strictFloatingPoint` on, the native map constructors decline a
-- floating-point key type, which they compare by its raw bits where Spark 4.0+ normalizes it (see
-- the map_funcs expression audit). Each constructor goes a different way: `CometMapFromArrays` has
-- no codegen dispatcher, so the projection falls back to Spark, while `CometMapFromEntries` mixes
-- in `CodegenDispatchFallback`, so it stays in the Comet pipeline running Spark's own generated
-- code. A floating-point value type is not affected.

-- Config: spark.comet.exec.strictFloatingPoint=true

statement
CREATE TABLE test_map_builders_strict_fp(k double, v int) USING parquet

statement
INSERT INTO test_map_builders_strict_fp VALUES (1.0D, 1), (double('-0.0'), 2), (2.5D, 3)

query expect_fallback(Map construction on a floating-point key)
SELECT map_from_arrays(array(k), array(v)) FROM test_map_builders_strict_fp

query expect_dispatch(map_from_entries)
SELECT map_from_entries(array(struct(k, v))) FROM test_map_builders_strict_fp

query expect_native(map_from_arrays)
SELECT map_from_arrays(array(v), array(k)) FROM test_map_builders_strict_fp

query expect_native(map_from_entries)
SELECT map_from_entries(array(struct(v, k))) FROM test_map_builders_strict_fp

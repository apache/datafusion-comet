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

-- map(...) constructor routed through the codegen dispatcher. Map keys may not be null in Spark.

statement
CREATE TABLE test_create_map(k string, v int) USING parquet

statement
INSERT INTO test_create_map VALUES ('a', 1), ('b', 2), ('c', NULL)

query
SELECT map(k, v) FROM test_create_map

query
SELECT map(1, 'a', 2, 'b'), map('x', array(1, 2), 'y', array(3))

-- ===== untyped constructors leave NullType children =====
-- `map()` is MapType(NullType, NullType) and `map(k, NULL)` is MapType(_, NullType).

query
SELECT map()

query
SELECT map('a', NULL)

query
SELECT map(k, NULL) FROM test_create_map

query
SELECT size(map()), size(map('a', NULL))

query
SELECT map_keys(map()), map_values(map('a', NULL))

query
SELECT array(map()), struct(map(), map('a', NULL))

query
SELECT map('a', array(NULL)), map('a', map())

-- carry the NullType children through a sort so the vector survives copy/spill paths
query
SELECT k, map(), map('a', NULL) FROM test_create_map ORDER BY k

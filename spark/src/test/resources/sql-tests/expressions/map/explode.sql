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

-- Config: spark.comet.exec.explode.enabled=true

statement
CREATE TABLE explode_maps(id int, m map<string, int>) USING parquet

statement
INSERT INTO explode_maps VALUES
  (1, map('b', 20, 'a', 10)), (2, map()), (3, NULL), (4, map('null', NULL))

query
SELECT id, explode(m) FROM explode_maps

query
SELECT id, m, explode_outer(m) FROM explode_maps

query
SELECT id, posexplode(m) FROM explode_maps

query
SELECT id, m, posexplode_outer(m) FROM explode_maps

query
SELECT posexplode_outer(m) FROM explode_maps

query
SELECT id, k, v FROM explode_maps LATERAL VIEW OUTER explode(m) e AS k, v
WHERE k IS NULL OR v IS NULL

query
SELECT id, p, k, v FROM explode_maps LATERAL VIEW OUTER posexplode(m) e AS p, k, v
WHERE p = 0 OR p IS NULL

-- Computed and scalar maps, including non-nullable values and nested fields.
query
SELECT id, posexplode(map(id, named_struct('v', coalesce(id, 0)))) FROM explode_maps

query
SELECT id, explode(map(1, 10, 2, 20)) FROM explode_maps

query
SELECT id, posexplode_outer(cast(NULL AS map<string, int>)) FROM explode_maps

-- The untyped map() constructor still falls back before the cast.
query expect_fallback(MapType(NullType,NullType,false))
SELECT id, explode_outer(cast(map() AS map<string, int>)) FROM explode_maps

statement
CREATE TABLE explode_nested_maps(
  id int, m map<array<int>, struct<a: array<int>, m: map<string, binary>>>) USING parquet

statement
INSERT INTO explode_nested_maps VALUES
  (1, map(array(2, 1), named_struct('a', array(10, NULL), 'm', map('x', unhex('FF00'))),
          array(3), NULL)),
  (2, map()), (3, NULL)

query
SELECT id, explode(m) FROM explode_nested_maps

query
SELECT id, explode_outer(m) FROM explode_nested_maps

query
SELECT id, posexplode(m) FROM explode_nested_maps

query
SELECT id, posexplode_outer(m) FROM explode_nested_maps

-- Project the output fields and compose map expansion with array expansion.
query
SELECT id, k, v.a, v.m FROM explode_nested_maps LATERAL VIEW OUTER explode(m) e AS k, v

query
SELECT id, p, k, item FROM explode_nested_maps
LATERAL VIEW OUTER posexplode(m) e AS p, k, v
LATERAL VIEW OUTER explode(v.a) a AS item

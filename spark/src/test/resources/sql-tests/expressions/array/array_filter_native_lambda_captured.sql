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

-- A lambda body that reads a captured column, or calls an array function with the lambda
-- parameter as an argument. The native side re-indexes the columns that the body reads and
-- evaluates the later arguments of those functions on a projection of them. A captured string,
-- binary or nested column is copied once per element by DataFusion, so a body that reads one
-- stays on the dispatcher.

-- ConfigMatrix: spark.sql.ansi.enabled=true,false

statement
CREATE TABLE sc2(id int, ints array<int>, other array<int>, m map<int,int>, s struct<a:int,b:string>, c int) USING parquet

statement
INSERT INTO sc2 VALUES (1, array(1, 2, 3, NULL), array(2, 3), map(1, 10, 2, 20), named_struct('a', 2, 'b', 'x'), 1),
  (2, array(5, 6), array(), map(), named_struct('a', 9, 'b', 'y'), 2),
  (3, NULL, array(1), NULL, NULL, 3),
  (4, array(1, 2), NULL, map(2, 5), named_struct('a', 1, 'b', NULL), 4)

query expect_dispatch(filter)
SELECT id, filter(ints, x -> array_contains(other, x)) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> x < size(other) + 2) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> try_element_at(other, 1) = x) FROM sc2

query expect_native(filter)
SELECT id, filter(ints, x -> element_at(map(1, 1, 2, 2), 2) = x) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> s.a = x) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> coalesce(try_element_at(other, 1), 0) = x) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> x = c AND s.b IS NOT NULL) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> m[x] > 5) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> array_position(other, x) > 0) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> array_contains(other, x + 1)) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> array_contains(other, cast(x as bigint))) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> try_element_at(other, x) IS NOT NULL) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> CASE WHEN x > c THEN x < size(other) ELSE array_contains(other, x) END) FROM sc2

query expect_dispatch(filter)
SELECT id, filter(ints, x -> arrays_overlap(other, array(x, c))) FROM sc2

query expect_native(filter)
SELECT id, filter(ints, x -> array_contains(array(c, 2, 3), x)) FROM sc2

query expect_native(filter)
SELECT id, filter(ints, x -> array_contains(array(c, 2, 3), x + 1)) FROM sc2

query expect_native(filter)
SELECT id, filter(ints, x -> array_position(array(c, 2), x) > 0) FROM sc2

query expect_native(filter)
SELECT id, filter(ints, x -> try_element_at(array(c, 5), x) > 1) FROM sc2

query expect_native(filter)
SELECT id, filter(ints, x -> arrays_overlap(array(c, 2), array(x, c))) FROM sc2

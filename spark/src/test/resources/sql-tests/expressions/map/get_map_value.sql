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

statement
CREATE TABLE test_map(m map<string, int>) USING parquet

statement
INSERT INTO test_map VALUES (map('a', 1, 'b', 2, 'c', 3)), (map('x', 10)), (NULL)

query spark_answer_only
SELECT m['a'], m['b'], m['c'] FROM test_map

query spark_answer_only
SELECT m['x'], m['missing'] FROM test_map

-- literal arguments
query spark_answer_only
SELECT map('a', 1, 'b', 2)['a'], map('a', 1, 'b', 2)['missing'], map('a', 1, 'b', 2)[NULL]

-- Map key types whose Spark equality native `map_extract` cannot reproduce are dispatched.
-- `x[key]` routes through `GetMapValue` / `CometMapExtract` (the `element_at` form in
-- `element_at_map.sql` exercises the same guard on `CometElementAt`). A `map<double,int>` column
-- is used rather than a `map(...)` literal because Spark's `SimplifyExtractValueOps` rewrites
-- `map(...)[key]` over a literal map into a `CASE` before it can reach the native lookup. Spark
-- compares with `nanSafeCompareDoubles`, so a `-0.0` lookup finds the `+0.0` key and returns `7`;
-- native lookup compares the raw Arrow values.
-- `-0.0D` because a decimal `-0.0` has no signed zero.
statement
CREATE TABLE test_map_double(m map<double, int>) USING parquet

statement
INSERT INTO test_map_double VALUES (map(CAST(0 AS DOUBLE), 7)), (map(CAST(1 AS DOUBLE), 8)), (NULL)

query expect_dispatch(getmapvalue)
SELECT m[-0.0D] FROM test_map_double

-- A NaN key is found by a NaN lookup, as `nanSafeCompareDoubles` treats NaN as equal to itself.
statement
INSERT INTO test_map_double VALUES (map(CAST('NaN' AS DOUBLE), 9))

query expect_dispatch(getmapvalue)
SELECT m[CAST('NaN' AS DOUBLE)] FROM test_map_double

-- A reflected method can access static fields shared by the two lookups.
query expect_fallback(a reflected method can share static state)
SELECT m[CAST(reflect('java.lang.Integer', 'toString', 0) AS DOUBLE)],
       m[CAST(reflect('java.lang.Integer', 'toString', 1) AS DOUBLE)]
FROM test_map_double

statement
CREATE TABLE test_map_struct_key(m map<struct<a:int, b:string>, int>, k int) USING parquet

statement
INSERT INTO test_map_struct_key VALUES
  (map(named_struct('a', 1, 'b', 'x'), 7), 1),
  (map(named_struct('a', 2, 'b', 'y'), 8), 3),
  (map(named_struct('a', 3, 'b', 'z'), 9), NULL),
  (NULL, 1)

query expect_dispatch(getmapvalue)
SELECT m[named_struct('a', k, 'b', 'x')],
       m[named_struct('a', k, 'b', CAST(NULL AS STRING))],
       m[CAST(NULL AS STRUCT<a:int, b:string>)]
FROM test_map_struct_key

-- With ANSI on the bounds check throws, so this non-nullable lookup needs no null flag.
statement
SET spark.sql.ansi.enabled=true

query expect_dispatch(getmapvalue)
SELECT m[element_at(array(named_struct('a', k, 'b', 'x')), 1)] FROM test_map_struct_key

statement
CREATE TABLE test_map_interpreted(
  m1 map<double, int>, m2 map<array<bigint>, int>, x int, n int, r string)
USING parquet

statement
INSERT INTO test_map_interpreted VALUES (map(1D, 7), map(array(1L), 8), 0, NULL, '1')

-- CollectList.update calls eval. With a nullable replacement, array_join.eval returns '1'
-- where Spark's generated code before 4.2 returns NULL (SPARK-57200).
query expect_dispatch(getmapvalue)
SELECT collect_list(m1[CAST(array_join(array(CAST(NULL AS STRING)), ',', r) AS DOUBLE)])
FROM test_map_interpreted

-- array_repeat.eval checks n first, so a NULL count skips the division by zero.
query expect_dispatch(getmapvalue)
SELECT collect_list(m2[array_repeat(10 DIV x, n)]) FROM test_map_interpreted

-- Spark disables its compile-failure retry during tests. Use its interpreted projection
-- as the reference for the non-ANSI bounds check, which assigns an undeclared null flag.
statement
SET spark.sql.ansi.enabled=false

statement
SET spark.sql.codegen.wholeStage=false

statement
SET spark.sql.codegen.factoryMode=NO_CODEGEN

query expect_fallback(non-nullable array element_at)
SELECT m[element_at(array(named_struct('a', k, 'b', 'x')), 1)] FROM test_map_struct_key

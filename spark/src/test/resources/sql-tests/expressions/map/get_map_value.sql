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

-- Config: spark.sql.subexpressionElimination.enabled=true

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
  m1 map<double, int>, m2 map<array<bigint>, int>, x int, n int, r string, a array<int>)
USING parquet

statement
INSERT INTO test_map_interpreted
SELECT map(1D, 7), map(array(1L), 8), id, IF(id = 0, NULL, 1), '1', IF(id = 0, NULL, array(1))
FROM range(0, 2, 1, 1)

-- Only the second row matches. A second lookup for the result starts its counter at zero.
query expect_fallback(can evaluate this expression on different rows)
SELECT coalesce(m1[CAST(monotonically_increasing_id() AS DOUBLE)], 0) FROM test_map_interpreted

-- parse_url.eval evaluates the part on a NULL URL. Spark 4.x Invoke codegen skips it.
query expect_error(DIVIDE_BY_ZERO)
SELECT collect_list(parse_url(CAST(n AS STRING), CAST(m1[1D / x] AS STRING)))
FROM test_map_interpreted

-- Spark skips the division on the first row in the comparisons and the filtered count.
query expect_fallback(can evaluate this expression on different rows)
SELECT n = m1[1D / x] FROM test_map_interpreted

query expect_fallback(can evaluate this expression on different rows)
SELECT n <> m1[1D / x] FROM test_map_interpreted

query expect_fallback(can evaluate this expression on different rows)
SELECT m1[CAST(n AS DOUBLE)] = 1D / x FROM test_map_interpreted

query expect_fallback(can evaluate this expression on different rows)
SELECT n, count(m1[1D / x]) FILTER (WHERE x > 0) FROM test_map_interpreted GROUP BY n

-- Dispatch in the count would make the grouped sum divide by x before its filter excludes zero.
query expect_fallback(sum can evaluate an argument on different rows)
SELECT n, count(m1[1D]), sum(1D / x) FILTER (WHERE x > 0)
FROM test_map_interpreted GROUP BY n

-- Spark also skips the division inside the filter when n is NULL.
query expect_fallback(sum can evaluate an argument on different rows)
SELECT n, count(m1[1D]), sum(x) FILTER (WHERE n = 1D / x)
FROM test_map_interpreted GROUP BY n

query expect_dispatch(getmapvalue)
SELECT n, count(m1[1D]), sum(x) FILTER (WHERE x > 0) FROM test_map_interpreted GROUP BY n

-- The second count skips the division when its first argument is NULL.
query expect_fallback(count can evaluate an argument on different rows)
SELECT count(m1[1D]), count(n, 1D / x) FROM test_map_interpreted

-- Spark hoists the repeated division above the array's NULL check.
query expect_error(DIVIDE_BY_ZERO)
SELECT element_at(a, m1[1D / x + 1D / x]) FROM test_map_interpreted

query expect_dispatch(getmapvalue)
SELECT collect_list(m1[1D]), collect_list(m1[CAST(x AS DOUBLE)]) FROM test_map_interpreted

query expect_dispatch(getmapvalue)
SELECT count(m1[1D]), sort_array(collect_list(x + 1)) FROM test_map_interpreted

-- The interpreted comparison skips the division when n is NULL.
query expect_fallback(collect_list can evaluate an argument on different rows)
SELECT count(m1[1D]), collect_list(n = 1D / x) FROM test_map_interpreted

query expect_fallback(last can evaluate an argument on different rows)
SELECT count(m1[1D]), last(n = 1D / x) FROM test_map_interpreted

query expect_dispatch(getmapvalue)
SELECT collect_list(m[named_struct('a', k, 'b', 'x')]) FROM test_map_struct_key

-- CollectList.update calls eval. With a nullable replacement, array_join.eval returns '1'
-- where Spark's generated code before 4.2 returns NULL (SPARK-57200).
query expect_fallback(Spark may interpret computed arguments)
SELECT collect_list(m1[CAST(array_join(array(CAST(NULL AS STRING)), ',', r) AS DOUBLE)])
FROM test_map_interpreted

-- array_repeat.eval checks n first, so a NULL count skips the division by zero.
query expect_fallback(Spark may interpret computed arguments)
SELECT collect_list(m2[array_repeat(10 DIV x, n)]) FROM test_map_interpreted

-- Spark disables its compile-failure retry during tests. Use its interpreted projection
-- as the reference for the non-ANSI bounds check, which assigns an undeclared null flag.
statement
SET spark.sql.ansi.enabled=false

statement
SET spark.sql.codegen.wholeStage=false

statement
SET spark.sql.codegen.factoryMode=NO_CODEGEN

query expect_fallback(non-nullable array lookup)
SELECT m[element_at(array(named_struct('a', k, 'b', 'x')), 1)] FROM test_map_struct_key

-- Spark only simplifies bracket indexing into an array constructor for an INT literal.
query expect_fallback(non-nullable array lookup)
SELECT m[array(named_struct('a', k, 'b', 'x'))[0L]] FROM test_map_struct_key

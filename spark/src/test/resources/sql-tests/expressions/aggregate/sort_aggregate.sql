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

-- Disabling ObjectHashAggregate forces Spark to plan SortAggregateExec for the
-- TypedImperativeAggregate function collect_set. collect_set.sql runs its whole fixture through
-- SortAggregateExec as well, so this file keeps only the shapes it does not cover. collect_set
-- returns elements in nondeterministic order, so every result is wrapped in sort_array.
-- Config: spark.sql.execution.useObjectHashAggregateExec=false

statement
CREATE TABLE sa_int(i int, g string) USING parquet

statement
INSERT INTO sa_int VALUES
  (1, 'a'), (2, 'a'), (1, 'a'), (3, 'a'),
  (4, 'b'), (4, 'b'), (NULL, 'b'), (5, 'b'),
  (NULL, 'c'), (NULL, 'c')

statement
CREATE TABLE sa_multikey(a int, k1 string, k2 int) USING parquet

statement
INSERT INTO sa_multikey VALUES
  (1, 'x', 10), (2, 'x', 10), (1, 'x', 10),
  (3, 'x', 20), (4, 'y', 10), (NULL, 'y', 10)

-- ============================================================
-- Multiple grouping keys (sort ordering spans both keys)
-- ============================================================

query
SELECT k1, k2, sort_array(collect_set(a))
FROM sa_multikey GROUP BY k1, k2 ORDER BY k1, k2

-- ============================================================
-- Grouping key derived from an expression
-- ============================================================

query
SELECT upper(g) AS gg, sort_array(collect_set(i))
FROM sa_int GROUP BY upper(g) ORDER BY gg

-- ============================================================
-- collect_set mixed with hashable aggregates: collect_set forces the
-- whole aggregate onto SortAggregate, so count/sum/min/max ride along
-- ============================================================

query
SELECT g, sort_array(collect_set(i)), count(*), count(i), sum(i), min(i), max(i)
FROM sa_int GROUP BY g ORDER BY g

-- ============================================================
-- Decimal SUM at maximum precision falls back: depending on the buffer
-- schema and codegen, Spark's sort aggregation keeps the running sum
-- unbounded or nulls it on overflow, and Comet does not track which
-- ============================================================

statement
CREATE TABLE sa_dec38(d decimal(38,0), i int, g string) USING parquet

statement
INSERT INTO sa_dec38 VALUES (1, 1, 'a'), (2, 2, 'a'), (3, 3, 'b'), (NULL, 4, 'b')

query expect_fallback(Decimal SUM at maximum precision cannot match Spark's sort aggregation buffer)
SELECT g, sum(d), sort_array(collect_set(i)) FROM sa_dec38 GROUP BY g ORDER BY g

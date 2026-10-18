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

-- ColumnPruning leaves a global aggregate with neither keys nor functions when nothing reads its
-- output. Each stage of the partial/final pair emits one row with no columns per partition, even
-- for an empty input.
-- Config: spark.comet.exec.localTableScan.enabled=true

-- Five files. With the test session's parallelism of 5, the scan reads them as five partitions, so
-- the final stage collapses five rows into one.
statement
CREATE TABLE empty_agg_multi(a int) USING parquet

statement
INSERT INTO empty_agg_multi SELECT CAST(id AS INT) FROM range(0, 10, 1, 5)

-- No files at all.
statement
CREATE TABLE empty_agg_none(a int) USING parquet

statement
CREATE TABLE empty_agg_division(n int, d int) USING parquet

statement
INSERT INTO empty_agg_division SELECT 1, 0

-- CollapseProject folds the outer SELECT into the aggregate, so the final stage projects the
-- literal over the row with no columns.
query
SELECT 1 FROM (SELECT count(*) FROM empty_agg_multi)

query
SELECT 1 FROM (SELECT count(*) FROM empty_agg_multi WHERE a > 100)

query
SELECT 1 FROM (SELECT count(*) FROM empty_agg_none)

-- Dataset.count() over an aggregated DataFrame plans this shape.
query
SELECT count(*) FROM (SELECT sum(a) FROM empty_agg_multi)

query
SELECT count(*) FROM (SELECT sum(a) FROM empty_agg_none)

query
SELECT 1 FROM (SELECT count(*) FROM VALUES (1) AS v(a))

query
SELECT 1 FROM (SELECT count(*) FROM VALUES (1) AS v(a) WHERE a > 5)

-- The parser plans HAVING without GROUP BY as a global aggregate, here one that outputs a literal.
query
SELECT 1 AS x FROM empty_agg_multi HAVING true

query
SELECT 1 AS x FROM empty_agg_none HAVING true

-- The aggregate still reads its whole input, so an error below it surfaces as it does in Spark.
statement
SET spark.sql.ansi.enabled=true

query expect_error_class(DIVIDE_BY_ZERO)
SELECT 1 FROM (SELECT count(*) FROM empty_agg_division WHERE n / d > 0)

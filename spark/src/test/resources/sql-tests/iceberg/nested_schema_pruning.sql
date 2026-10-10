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

-- The native Iceberg scan reads only the nested fields that Spark's nested schema pruning keeps.
-- These queries check the pruned reads against Spark. CometIcebergNativeSuite checks that they
-- scan fewer bytes, and covers deletes, time travel, and files without field ids.

-- No Iceberg spark-runtime is published for Spark 4.2 yet, and the 4.0 runtime the build reuses
-- is binary-incompatible with it. See https://github.com/apache/datafusion-comet/issues/4969.
-- MaxSparkVersion: 4.1

-- Config: spark.sql.catalog.test_cat=org.apache.iceberg.spark.SparkCatalog
-- Config: spark.sql.catalog.test_cat.type=hadoop
-- Config: spark.sql.catalog.test_cat.warehouse=/tmp/comet-iceberg-sql-test
-- Config: spark.comet.scan.icebergNative.enabled=true

statement
DROP TABLE IF EXISTS test_cat.db.nested_pruning

statement
CREATE TABLE test_cat.db.nested_pruning (
  id INT,
  s STRUCT<a: INT, pad: STRING, inner: STRUCT<b: INT, pad: STRING>>,
  items ARRAY<STRUCT<x: INT, pad: STRING>>,
  m MAP<STRING, STRUCT<v: INT, pad: STRING>>
) USING iceberg

-- NULL structs, lists, maps, list elements, map values, and fields, so that the pruned read has
-- to rebuild the validity of every level from the leaves it keeps.
statement
INSERT INTO test_cat.db.nested_pruning
SELECT
  CAST(id AS INT),
  IF(id % 7 = 0, NULL, named_struct(
    'a', IF(id % 5 = 0, NULL, CAST(id AS INT)),
    'pad', repeat('p', 20),
    'inner', IF(id % 3 = 0, NULL, named_struct('b', CAST(id * 2 AS INT), 'pad', repeat('q', 20))))),
  IF(id % 7 = 0, NULL, array(
    named_struct('x', CAST(id AS INT), 'pad', repeat('r', 20)),
    IF(id % 5 = 0, NULL, named_struct('x', CAST(-id AS INT), 'pad', repeat('s', 20))))),
  IF(id % 7 = 0, NULL, map('k', IF(id % 3 = 0, NULL,
    named_struct('v', CAST(id AS INT), 'pad', repeat('t', 20)))))
FROM range(100)

query
SELECT id, s.a FROM test_cat.db.nested_pruning ORDER BY id

-- `s IS NULL` reads the validity of `s` pruned to `s.inner.b`.
query
SELECT id, s.inner.b, s IS NULL FROM test_cat.db.nested_pruning ORDER BY id

query
SELECT id, items.x FROM test_cat.db.nested_pruning ORDER BY id

query
SELECT id, m['k'].v FROM test_cat.db.nested_pruning ORDER BY id

query
SELECT id FROM test_cat.db.nested_pruning WHERE s.inner.b > 100 ORDER BY id

-- The whole struct, so nothing is pruned.
query
SELECT id, s FROM test_cat.db.nested_pruning ORDER BY id

statement
DROP TABLE test_cat.db.nested_pruning

-- A partition source the query does not project is appended to the task schema at its top level.
-- `s.region` is nested, so a query that prunes it away reads with the full table schema instead,
-- where an appended `region` would clash with the top-level one.
statement
DROP TABLE IF EXISTS test_cat.db.nested_pruning_nested_source

statement
CREATE TABLE test_cat.db.nested_pruning_nested_source (
  id INT, region STRING, s STRUCT<region: STRING, a: INT, pad: STRING>
) USING iceberg PARTITIONED BY (s.region)

statement
INSERT INTO test_cat.db.nested_pruning_nested_source
SELECT CAST(id AS INT), 'top', named_struct(
  'region', IF(id % 2 = 0, 'east', 'west'), 'a', CAST(id AS INT), 'pad', repeat('p', 20))
FROM range(100)

query
SELECT id, region, s.a FROM test_cat.db.nested_pruning_nested_source ORDER BY id

query
SELECT id, s.region FROM test_cat.db.nested_pruning_nested_source ORDER BY id

statement
DROP TABLE test_cat.db.nested_pruning_nested_source

-- The partition source `p` is renamed to `q`, and a new `p` is added. A query that does not
-- project `q` appends it to the task schema under its current name, apart from the new `p`. A
-- bucket partition, because Iceberg rejects a new column named like an identity partition field.
statement
DROP TABLE IF EXISTS test_cat.db.nested_pruning_renamed_source

statement
CREATE TABLE test_cat.db.nested_pruning_renamed_source (
  id INT, p STRING, s STRUCT<a: INT, pad: STRING>
) USING iceberg PARTITIONED BY (bucket(4, p))

statement
INSERT INTO test_cat.db.nested_pruning_renamed_source
SELECT CAST(id AS INT), CAST(id AS STRING), named_struct(
  'a', CAST(id AS INT), 'pad', repeat('p', 20))
FROM range(100)

statement
ALTER TABLE test_cat.db.nested_pruning_renamed_source RENAME COLUMN p TO q

statement
ALTER TABLE test_cat.db.nested_pruning_renamed_source ADD COLUMN p STRING

query
SELECT id, p, s.a FROM test_cat.db.nested_pruning_renamed_source ORDER BY id

statement
DROP TABLE test_cat.db.nested_pruning_renamed_source

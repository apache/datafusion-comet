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

-- Confirms Comet falls back to Spark when a parquet scan's schema contains a
-- VariantType column. VariantType is a Spark 4.0+ data type that Comet does
-- not currently support, so any scan exposing it must be executed by Spark.

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_variant(id INT, v VARIANT, tail STRING) USING parquet

statement
INSERT INTO test_variant VALUES
  (1, parse_json('{"a": 1, "b": "hello"}'), 'first'),
  (2, parse_json('{"a": 2, "b": "world"}'), NULL),
  (3, parse_json('null'), 'variant-null'),
  (4, NULL, 'sql-null')

-- A plain Parquet scan can remain native when its required schema prunes the
-- Variant column completely, including both SQL NULL and Variant null values.
query
SELECT id FROM test_variant ORDER BY id

-- A projected column after the pruned Variant must use its rebased native index.
query
SELECT tail FROM test_variant ORDER BY id

query
SELECT id, tail FROM test_variant WHERE tail IS NOT NULL ORDER BY id

query expect_fallback(type VariantType)
SELECT id, v FROM test_variant ORDER BY id

query expect_fallback(type VariantType)
SELECT variant_get(v, '$.a', 'int') AS a FROM test_variant ORDER BY id

query expect_fallback(type VariantType)
SELECT id FROM test_variant WHERE variant_get(v, '$.a', 'int') = 1

query expect_fallback(type VariantType)
SELECT COUNT(*) FROM test_variant WHERE v IS NOT NULL

statement
CREATE TABLE test_variant_struct(id INT, s STRUCT<safe: INT, v: VARIANT>, tail STRING)
USING parquet

statement
INSERT INTO test_variant_struct VALUES
  (1, named_struct('safe', 10, 'v', parse_json('{"x": 10}')), 'first'),
  (2, named_struct('safe', NULL, 'v', parse_json('{"x": 20}')), NULL),
  (3, NULL, 'null-parent')

query
SELECT id FROM test_variant_struct ORDER BY id

query
SELECT tail FROM test_variant_struct ORDER BY id

-- Projecting a supported sibling replaces the full struct with Spark's pruned nested schema.
query
SELECT s.safe FROM test_variant_struct ORDER BY id

query expect_fallback(type VariantType)
SELECT id, s FROM test_variant_struct ORDER BY id

statement
CREATE TABLE test_variant_partitioned(id INT, v VARIANT, tail STRING, p INT)
USING parquet PARTITIONED BY (p)

statement
INSERT INTO test_variant_partitioned VALUES
  (1, parse_json('{"a": 1}'), 'first', 10),
  (2, NULL, 'second', 20)

-- Partition columns are appended after the pruned data schema, so their offsets must be rebased.
query
SELECT tail, p FROM test_variant_partitioned ORDER BY id

-- File-constant metadata follows the partition columns and needs the same rebased offsets.
query
SELECT tail, p, _metadata.file_name FROM test_variant_partitioned ORDER BY id

statement
CREATE TABLE test_plain_variant_shape(id INT, payload STRUCT<value: BINARY, metadata: BINARY>)
USING parquet

statement
INSERT INTO test_plain_variant_shape VALUES
  (1, named_struct('value', X'01', 'metadata', X'02')),
  (2, NULL)

-- An ordinary binary struct with Variant-like field names is not a logical Variant.
query
SELECT payload FROM test_plain_variant_shape ORDER BY id

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

-- Confirms direct top-level VariantType projection through Comet's ordinary
-- native Parquet scan. Expressions, operators, nested Variant, and Iceberg
-- remain unsupported.

-- MinSparkVersion: 4.0
-- Config: spark.sql.variant.writeShredding.enabled=false
-- Config: spark.sql.variant.pushVariantIntoScan=false
-- Config: spark.sql.variant.allowReadingShredded=true
-- Config: spark.sql.variant.forceShreddingSchemaForTest=k00 BIGINT

statement
CREATE TABLE test_variant(id INT, v VARIANT, tail STRING) USING parquet

statement
INSERT INTO test_variant VALUES
  (1, parse_json('{"b": "hello", "a": 1}'), 'object'),
  (2, parse_json('[1, true, "x"]'), 'array'),
  (3, parse_json('42'), 'scalar'),
  (4, parse_json('null'), 'json-null'),
  (5, CAST(NULL AS VARIANT), 'sql-null'),
  (6, parse_json('{"":1,"nested":{"":2}}'), 'empty-key')

-- A plain Parquet scan can remain native when its required schema prunes the
-- Variant column completely, including both SQL NULL and Variant null values.
query
SELECT id FROM test_variant ORDER BY id

-- A projected column after the pruned Variant must use its rebased native index.
query
SELECT tail FROM test_variant ORDER BY id

query
SELECT id, tail FROM test_variant WHERE tail IS NOT NULL ORDER BY id

-- Full-value projection is scan-only: no native expression or pass-through operator carries v.
query
SELECT v FROM test_variant

query
SELECT id, v, tail FROM test_variant

-- Spark's pushed VariantStruct remains an explicit fallback in Phase A.
statement
SET spark.sql.variant.pushVariantIntoScan=true

query expect_fallback(shredded; not supported by native scan)
SELECT v FROM test_variant

statement
SET spark.sql.variant.pushVariantIntoScan=false

query expect_fallback(type VariantType)
SELECT v FROM test_variant ORDER BY id

query expect_fallback(type VariantType)
SELECT v FROM test_variant LIMIT 1

query expect_fallback(type VariantType)
SELECT /*+ REPARTITION(2, id) */ id, v FROM test_variant

query expect_fallback(type VariantType)
SELECT variant_get(v, '$.a', 'int') AS a FROM test_variant ORDER BY id

query expect_fallback(type VariantType)
SELECT id FROM test_variant WHERE variant_get(v, '$.a', 'int') = 1

query expect_fallback(type VariantType)
SELECT COUNT(*) FROM test_variant WHERE v IS NOT NULL

query expect_fallback(type VariantType)
SELECT CAST(v AS STRING) FROM test_variant

-- A Variant existence default is read from Spark's table schema and applied only when an old
-- Parquet file does not contain the column. variant_get remains a Spark expression, while the
-- ordinary Parquet scan and missing-column substitution stay native.
statement
CREATE TABLE test_variant_defaults_sql(id INT) USING parquet

statement
INSERT INTO test_variant_defaults_sql VALUES (1)

statement
ALTER TABLE test_variant_defaults_sql ADD COLUMNS(
  v VARIANT DEFAULT parse_json('{"a":1}'), n INT DEFAULT 7)

statement
INSERT INTO test_variant_defaults_sql VALUES (2, parse_json('{"a":2}'), 8)

statement
SET spark.sql.parquet.enableVectorizedReader=false

statement
SET spark.comet.scan.allowDisabledParquetVectorizedReader=true

query expect_fallback(type VariantType)
SELECT id, variant_get(v, '$.a', 'int') AS a, n
FROM test_variant_defaults_sql ORDER BY id

statement
SET spark.sql.parquet.enableVectorizedReader=true

statement
SET spark.comet.scan.allowDisabledParquetVectorizedReader=false

-- Arrow and Spark order supplementary Unicode object keys differently. Force top-level and nested
-- shredded fields so the native scan reconstructs their residual values before Spark's lookup.
statement
SET spark.sql.variant.forceShreddingSchemaForTest=k00 BIGINT, nested STRUCT<known: BIGINT>

statement
SET spark.sql.variant.writeShredding.enabled=true

statement
CREATE TABLE test_variant_unicode(v VARIANT) USING parquet

statement
INSERT INTO test_variant_unicode VALUES
  (parse_json(
    '{"k00":0,"k01":1,"k02":2,"k03":3,"k04":4,"k05":5,"k06":6,"k07":7,"k08":8,"k09":9,"k10":10,"k11":11,"k12":12,"k13":13,"k14":14,"k15":15,"k16":16,"k17":17,"k18":18,"k19":19,"k20":20,"k21":21,"k22":22,"k23":23,"k24":24,"k25":25,"k26":26,"k27":27,"k28":28,"k29":29,"\uE000":30,"😀":531}')),
  (parse_json(
    '{"":-2,"k00":0,"nested":{"known":99,"":-1,"k00":0,"k01":1,"k02":2,"k03":3,"k04":4,"k05":5,"k06":6,"k07":7,"k08":8,"k09":9,"k10":10,"k11":11,"k12":12,"k13":13,"k14":14,"k15":15,"k16":16,"k17":17,"k18":18,"k19":19,"k20":20,"k21":21,"k22":22,"k23":23,"k24":24,"k25":25,"k26":26,"k27":27,"k28":28,"k29":29,"\uE000":30,"😀":532}}'))

statement
SET spark.sql.variant.writeShredding.enabled=false

statement
SET spark.sql.variant.allowReadingShredded=true

query
SELECT v FROM test_variant_unicode

query expect_fallback(type VariantType)
SELECT variant_get(v, '$.😀', 'bigint'), variant_get(v, '$.nested.😀', 'bigint')
FROM test_variant_unicode

-- Spark rebuilds typed fields in physical shredding-schema order and chooses integer/decimal
-- widths from the runtime value, independently of the Parquet physical width.
statement
SET spark.sql.variant.forceShreddingSchemaForTest=b BIGINT, a BIGINT, d DECIMAL(38,2)

statement
SET spark.sql.variant.writeShredding.enabled=true

statement
CREATE TABLE test_variant_typed_bytes(v VARIANT) USING parquet

statement
INSERT INTO test_variant_typed_bytes VALUES (parse_json('{"a":1,"b":2,"d":1.23}'))

statement
SET spark.sql.variant.writeShredding.enabled=false

query
SELECT v FROM test_variant_typed_bytes

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
CREATE TABLE test_variant_collections(
  id INT,
  variants ARRAY<VARIANT>,
  variants_by_key MAP<STRING, VARIANT>,
  tail STRING)
USING parquet

statement
INSERT INTO test_variant_collections VALUES
  (1,
   array(parse_json('{"x": 1}'), parse_json('null')),
   map('first', parse_json('{"x": 2}')),
   'first'),
  (2,
   array(CAST(NULL AS VARIANT)),
   map('sql-null', CAST(NULL AS VARIANT)),
   NULL),
  (3, NULL, NULL, 'null-collections')

-- Variant-bearing arrays and maps can be pruned as entire top-level fields.
query
SELECT id, tail FROM test_variant_collections ORDER BY id

-- Exposing either collection still requires Spark to decode its nested Variant values.
query expect_fallback(type VariantType)
SELECT id, variants FROM test_variant_collections ORDER BY id

query expect_fallback(type VariantType)
SELECT id, variants_by_key FROM test_variant_collections ORDER BY id

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

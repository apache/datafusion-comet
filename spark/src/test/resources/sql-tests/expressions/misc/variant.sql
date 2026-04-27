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
CREATE TABLE test_variant(id INT, v VARIANT) USING parquet

statement
INSERT INTO test_variant VALUES
  (1, parse_json('{"a": 1, "b": "hello"}')),
  (2, parse_json('{"a": 2, "b": "world"}')),
  (3, parse_json('null')),
  (4, NULL)

-- IgnoreFromSparkVersion: 4.1 https://github.com/apache/datafusion-comet/issues/4098
query expect_fallback(Unsupported v of type VariantType)
SELECT id, v FROM test_variant ORDER BY id

query expect_fallback(Unsupported v of type VariantType)
SELECT variant_get(v, '$.a', 'int') AS a FROM test_variant ORDER BY id

query expect_fallback(Unsupported v of type VariantType)
SELECT id FROM test_variant WHERE variant_get(v, '$.a', 'int') = 1

query expect_fallback(Unsupported v of type VariantType)
SELECT COUNT(*) FROM test_variant WHERE v IS NOT NULL

statement
CREATE TABLE test_variant_struct(id INT, s STRUCT<v: VARIANT>) USING parquet

statement
INSERT INTO test_variant_struct VALUES
  (1, named_struct('v', parse_json('{"x": 10}'))),
  (2, named_struct('v', parse_json('{"x": 20}')))

query expect_fallback(Unsupported v of type VariantType)
SELECT id, s FROM test_variant_struct ORDER BY id

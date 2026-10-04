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

-- MinSparkVersion: 4.0
-- ConfigMatrix: parquet.enable.dictionary=false,true
-- Config: spark.sql.variant.allowReadingShredded=true
-- Config: spark.sql.variant.pushVariantIntoScan=false
-- Config: spark.sql.variant.writeShredding.enabled=false
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- Config: spark.comet.expression.VariantGet.allowIncompatible=false

statement
CREATE TABLE test_variant_get(id INT, v VARIANT, path STRING) USING parquet

statement
INSERT INTO test_variant_get VALUES
  (1, parse_json('{"a":[{"b":12},null,3],"n":-12.345,"bool":true,"":8,"a.b":9}'), '$.n'),
  (2, parse_json('{"a":{},"n":"19.50","bool":"false"}'), '$.bool'),
  (3, parse_json('{"n":null}'), '$.missing'),
  (4, parse_json('null'), '$'),
  (5, NULL, '$')

query expect_native(variant_get)
SELECT variant_get(v, '$.a[0].b', 'int'), variant_get(v, '$["a.b"]', 'bigint'),
       variant_get(v, '$[""]', 'tinyint') FROM test_variant_get

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$.n', 'float'), try_variant_get(v, '$.n', 'double'),
       try_variant_get(v, '$.bool', 'boolean') FROM test_variant_get

query expect_fallback(Floating-point to decimal rounding can differ from Spark on JDK 17)
SELECT try_variant_get(v, '$.n', 'decimal(10,1)') FROM test_variant_get

query expect_fallback(Date/time parsing and timezone conversion support a narrower year range than Spark)
SELECT try_variant_get(v, '$.n', 'date') FROM test_variant_get

query expect_fallback(Variant STRING extraction requires Spark-compatible JSON and scalar formatting)
SELECT variant_get(v, '$.a', 'string'), variant_get(v, '$.n', 'string') FROM test_variant_get

query expect_native(variant_get)
SELECT variant_get(v, '$.a[99]', 'smallint'), variant_get(v, '$.missing', 'int'),
       variant_get(v, '$.n.child', 'int') FROM test_variant_get

query expect_native(variant_get)
SELECT id FROM test_variant_get WHERE variant_get(v, '$.a[0].b', 'int') = 12

query expect_native(variant_get)
SELECT variant_get(v, concat('$.a', '[0].b'), 'int') FROM test_variant_get

query expect_fallback(Variant extraction requires a non-null foldable path)
SELECT try_variant_get(v, path, 'double') FROM test_variant_get

query expect_fallback(Variant extraction supports Boolean, numeric, binary, date and timestamp targets only)
SELECT try_variant_get(v, '$.a', 'array<int>') FROM test_variant_get

query expect_error(INVALID_VARIANT_GET_PATH)
SELECT try_variant_get(v, '$[-1]', 'int') FROM test_variant_get

statement
CREATE TABLE test_variant_get_casts(id INT, v VARIANT) USING parquet

statement
INSERT INTO test_variant_get_casts VALUES
  (1, parse_json('127')), (2, parse_json('128')), (3, parse_json('-129')),
  (4, parse_json('"bad"')), (5, parse_json('"1.5"')),
  (6, parse_json('9223372036854775807')), (7, parse_json('true')),
  (8, CAST(double('NaN') AS VARIANT)), (9, CAST(double('Infinity') AS VARIANT)),
  (10, parse_json('null')), (11, NULL)

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$', 'tinyint'), try_variant_get(v, '$', 'smallint'),
       try_variant_get(v, '$', 'bigint') FROM test_variant_get_casts

query expect_error(INVALID_VARIANT_CAST)
SELECT variant_get(v, '$', 'tinyint') FROM test_variant_get_casts WHERE id = 2

query expect_error(INVALID_VARIANT_CAST)
SELECT variant_get(v, '$', 'int') FROM test_variant_get_casts WHERE id = 4

-- Decimal and date/time extraction use the shared native casts, with their documented limits.
statement
SET spark.comet.expression.VariantGet.allowIncompatible=true

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$', 'decimal(3,1)'),
       try_variant_get(v, '$', 'timestamp') FROM test_variant_get_casts

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$.n', 'decimal(10,1)') FROM test_variant_get

statement
CREATE TABLE test_variant_get_temporal(id INT, v VARIANT) USING parquet

statement
INSERT INTO test_variant_get_temporal VALUES
  (1, CAST(timestamp'2024-03-10 01:30:00.123456' AS VARIANT)),
  (2, CAST(timestamp_ntz'2024-11-03 01:30:00.123456' AS VARIANT)),
  (3, CAST(date'2024-01-02' AS VARIANT)),
  (4, parse_json('"2024-03-10 02:30:00"')),
  (5, parse_json('-1.2345678')), (6, parse_json('9223372036855')),
  (7, parse_json('"not-a-date"')), (8, NULL)

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$', 'timestamp'), try_variant_get(v, '$', 'timestamp_ntz'),
       try_variant_get(v, '$', 'date') FROM test_variant_get_temporal

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$', 'tinyint'), try_variant_get(v, '$', 'smallint'),
       try_variant_get(v, '$', 'int'), try_variant_get(v, '$', 'bigint'),
       try_variant_get(v, '$', 'float'), try_variant_get(v, '$', 'double'),
       try_variant_get(v, '$', 'decimal(22,6)') FROM test_variant_get_temporal

statement
SET spark.sql.session.timeZone=GMT+8

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$', 'timestamp') FROM test_variant_get_temporal

statement
SET spark.sql.session.timeZone=+08:00:01

query expect_fallback(cannot be represented in native code)
SELECT try_variant_get(v, '$', 'timestamp') FROM test_variant_get_temporal

statement
SET spark.sql.session.timeZone=America/Los_Angeles

statement
CREATE TABLE test_variant_get_binary(v VARIANT) USING parquet

statement
INSERT INTO test_variant_get_binary VALUES
  (CAST(X'0102FF' AS VARIANT)), (parse_json('"text"')), (parse_json('1')), (NULL)

query expect_native(try_variant_get)
SELECT try_variant_get(v, '$', 'binary') FROM test_variant_get_binary

statement
SET spark.comet.expression.VariantGet.allowIncompatible=false

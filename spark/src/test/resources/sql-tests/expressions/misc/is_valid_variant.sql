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

-- MinSparkVersion: 4.2
-- Config: spark.sql.variant.allowReadingShredded=true
-- Config: spark.sql.variant.pushVariantIntoScan=false
-- Config: spark.sql.variant.writeShredding.enabled=false
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_is_valid_variant(id INT, v VARIANT) USING parquet

statement
INSERT INTO test_is_valid_variant VALUES
  (1, parse_json('null')),
  (2, NULL),
  (3, parse_json('true')),
  (4, parse_json('false')),
  (5, parse_json('42')),
  (6, parse_json('-9223372036854775808')),
  (7, parse_json('1.25')),
  (8, parse_json('1e100')),
  (9, parse_json('""')),
  (10, parse_json('"null"')),
  (11, parse_json('"中文"')),
  (12, parse_json('[]')),
  (13, parse_json('{}')),
  (14, parse_json('[1,null,{"a":true}]')),
  (15, parse_json('{"a":null,"b":[1,2]}')),
  (16, CAST(DATE '2024-01-01' AS VARIANT)),
  (17, CAST(TIMESTAMP '2024-01-01 12:34:56' AS VARIANT)),
  (18, CAST(X'0001FF' AS VARIANT))

query
SELECT id, is_valid_variant(v) FROM test_is_valid_variant

query
SELECT id, NOT is_valid_variant(v), is_valid_variant(v) IS NULL FROM test_is_valid_variant

query
SELECT id FROM test_is_valid_variant WHERE is_valid_variant(v)

query
SELECT id, v IS NULL, v IS NOT NULL FROM test_is_valid_variant

statement
SET spark.comet.expression.IsValidVariant.enabled=false

query expect_fallback(Expression support is disabled)
SELECT is_valid_variant(v) FROM test_is_valid_variant

statement
SET spark.comet.expression.IsValidVariant.enabled=true

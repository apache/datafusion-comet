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

-- Opt in to the native (rust) to_json path. Without this, to_json runs through the codegen
-- dispatcher (Spark's own implementation) instead.
-- Config: spark.comet.expression.StructsToJson.allowIncompatible=true
-- ignoreNullFields changes whether null struct fields are emitted, so exercise both values.
-- ConfigMatrix: spark.sql.jsonGenerator.ignoreNullFields=false,true

statement
CREATE TABLE test_to_json(
  i int, s string, f float, d double, bl boolean, bt tinyint, sh smallint, lng bigint)
USING parquet

statement
INSERT INTO test_to_json VALUES
  (1, 'hello', cast('NaN' as float), cast('Infinity' as double), true, cast(127 as tinyint), cast(32767 as smallint), 9223372036854775807),
  (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (0, '', cast(0.0 as float), 0.0, false, cast(0 as tinyint), cast(0 as smallint), 0),
  (-2147483648, 'say "hi"', cast('-Infinity' as float), cast('-Infinity' as double), true, cast(-128 as tinyint), cast(-32768 as smallint), -9223372036854775808),
  (2147483647, 'café 中文', cast('NaN' as float), cast('NaN' as double), false, cast(-1 as tinyint), cast(-1 as smallint), -1)

-- every natively-supported type as a top-level struct field
query
SELECT to_json(named_struct(
  'i', i, 's', s, 'f', f, 'd', d, 'bl', bl, 'bt', bt, 'sh', sh, 'lng', lng)) FROM test_to_json

-- each type individually, column argument
query
SELECT to_json(named_struct('i', i)) FROM test_to_json

query
SELECT to_json(named_struct('s', s)) FROM test_to_json

query
SELECT to_json(named_struct('f', f)) FROM test_to_json

query
SELECT to_json(named_struct('d', d)) FROM test_to_json

query
SELECT to_json(named_struct('bl', bl)) FROM test_to_json

query
SELECT to_json(named_struct('bt', bt)) FROM test_to_json

query
SELECT to_json(named_struct('sh', sh)) FROM test_to_json

query
SELECT to_json(named_struct('lng', lng)) FROM test_to_json

-- nested struct of supported types
query
SELECT to_json(named_struct(
  'inner', named_struct('i', i, 's', s, 'bl', bl),
  'lng', lng)) FROM test_to_json

-- deeply nested struct
query
SELECT to_json(named_struct(
  'a', named_struct('b', named_struct('c', i, 'd', s)))) FROM test_to_json

-- literal arguments covering each type
query
SELECT to_json(named_struct(
  'i', 1, 's', 'hello', 'f', cast(1.5 as float), 'd', 2.5,
  'bl', true, 'bt', cast(7 as tinyint), 'sh', cast(700 as smallint), 'lng', 9000000000))

-- literal struct with a value that requires JSON string escaping
query
SELECT to_json(named_struct('s', 'quote " and slash \\ and tab\there'))

-- to_json with options is not supported by the native (rust) path, so it routes to the codegen
-- dispatcher, which runs Spark's own implementation inside Comet (still native Comet execution).
query
SELECT to_json(named_struct('i', i, 's', s), map('timestampFormat', 'dd/MM/yyyy')) FROM test_to_json

-- to_json with an array field is not supported by the native (rust) path, so it likewise routes to
-- the codegen dispatcher.
query
SELECT to_json(named_struct('i', i, 'arr', array(s))) FROM test_to_json

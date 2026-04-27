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

-- Config: spark.comet.expression.GetJsonObject.allowIncompatible=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_get_json_object(json_str string, path string) USING parquet

statement
INSERT INTO test_get_json_object VALUES ('{"name":"John","age":30}', '$.name'), ('{"a":{"b":{"c":"deep"}}}', '$.a.b.c'), ('{"items":[1,2,3]}', '$.items[1]'), (NULL, '$.name'), ('{"name":"Alice"}', NULL), ('invalid json', '$.name'), ('{"a":"b"}', '$'), ('{"key":"value"}', '$.missing'), ('{"arr":["x","y","z"]}', '$.arr[0]'), ('{"a":null}', '$.a')

-- column json, column path
query
SELECT get_json_object(json_str, path) FROM test_get_json_object

-- column json, literal path
query
SELECT get_json_object(json_str, '$.name') FROM test_get_json_object

-- simple field extraction
-- IgnoreFromSparkVersion: 4.1 https://github.com/apache/datafusion-comet/issues/4098
query
SELECT get_json_object('{"name":"John","age":30}', '$.name')

-- numeric value
query
SELECT get_json_object('{"name":"John","age":30}', '$.age')

-- nested field
query
SELECT get_json_object('{"user":{"profile":{"name":"Alice"}}}', '$.user.profile.name')

-- array index
query
SELECT get_json_object('[1,2,3]', '$[0]'), get_json_object('[1,2,3]', '$[2]')

-- out of bounds array index
query
SELECT get_json_object('[1,2,3]', '$[3]')

-- nested array field
query
SELECT get_json_object('{"items":["apple","banana","cherry"]}', '$.items[1]')

-- root path returns entire input
query
SELECT get_json_object('{"a":"b"}', '$')

-- null json
query
SELECT get_json_object(NULL, '$.name')

-- null path
query
SELECT get_json_object('{"a":"b"}', NULL)

-- invalid json returns null
query
SELECT get_json_object('not json', '$.a')

-- missing field returns null
query
SELECT get_json_object('{"a":"b"}', '$.c')

-- json null value returns null
query
SELECT get_json_object('{"a":null}', '$.a')

-- boolean value
query
SELECT get_json_object('{"flag":true}', '$.flag')

-- nested object returned as json
query
SELECT get_json_object('{"a":{"b":1}}', '$.a')

-- array returned as json
query
SELECT get_json_object('{"a":[1,2,3]}', '$.a')

-- bracket notation
query
SELECT get_json_object('{"key with spaces":"it works"}', '$[''key with spaces'']')

-- wildcard on array
query
SELECT get_json_object('[{"a":"b"},{"a":"c"}]', '$[*].a')

-- empty string json
query
SELECT get_json_object('', '$.a')

-- deeply nested
query
SELECT get_json_object('{"a":{"b":{"c":{"d":"found"}}}}', '$.a.b.c.d')

-- array of arrays
query
SELECT get_json_object('[[1,2],[3,4]]', '$[0][1]')

-- wildcard on simple array returns the whole array
query
SELECT get_json_object('[1,2,3]', '$[*]')

-- object wildcard (Spark returns null for $.* on objects)
query
SELECT get_json_object('{"a":1,"b":2,"c":3}', '$.*')

-- single wildcard match returns unwrapped value
query
SELECT get_json_object('[{"a":"only"}]', '$[*].a')

-- wildcard with missing fields in some elements
query
SELECT get_json_object('[{"a":1},{"b":2},{"a":3}]', '$[*].a')

-- invalid path: recursive descent
query
SELECT get_json_object('{"a":1}', '$..a')

-- invalid path: no dollar sign
query
SELECT get_json_object('{"a":1}', 'a')

-- invalid path: dot-bracket
query
SELECT get_json_object('[1,2,3]', '$.[0]')

-- field name with special chars
query
SELECT get_json_object('{"fb:testid":"123"}', '$.fb:testid')

-- escaped quotes in string values
query
SELECT get_json_object('{"a":"b\\"c"}', '$.a')

-- numeric precision preserved
query
SELECT get_json_object('{"price":8.95}', '$.price')

-- object key ordering preserved in output
query
SELECT get_json_object('{"z":1,"a":2}', '$')

-- unicode field names and values
query
SELECT get_json_object('{"名前":"太郎","年齢":25}', '$.名前')

-- unicode values
query
SELECT get_json_object('{"greeting":"こんにちは世界"}', '$.greeting')

-- emoji in values
query
SELECT get_json_object('{"emoji":"🎉🚀"}', '$.emoji')

-- unicode bracket notation
query
SELECT get_json_object('{"键":"值"}', '$[''键'']')

-- mixed scripts (Latin with accents)
query
SELECT get_json_object('{"data":"café résumé naïve"}', '$.data')

-- unicode in wildcard results
query
SELECT get_json_object('[{"名":"Alice"},{"名":"太郎"}]', '$[*].名')

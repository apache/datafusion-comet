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

-- Opt in to the native from_json path. Duplicate struct field names cannot be represented by
-- Arrow without collapsing children, so those schemas must fall back to Spark.
-- Config: spark.comet.expression.JsonToStructs.allowIncompatible=true

statement
CREATE TABLE test_from_json_duplicate_fields(id int, j string) USING parquet

statement
INSERT INTO test_from_json_duplicate_fields VALUES
  (1, '{"a":1,"outer":{"a":2}}'),
  (2, '{"a":3,"outer":{"a":4}}')

query expect_fallback(unsupported output type)
SELECT from_json(j, 'a INT, a INT') FROM test_from_json_duplicate_fields ORDER BY id

query expect_fallback(unsupported output type)
SELECT from_json(j, 'outer STRUCT<a: INT, a: INT>')
FROM test_from_json_duplicate_fields ORDER BY id

-- Distinct field names remain on the native path.
query
SELECT from_json(j, 'a INT, outer STRUCT<a: INT>')
FROM test_from_json_duplicate_fields ORDER BY id

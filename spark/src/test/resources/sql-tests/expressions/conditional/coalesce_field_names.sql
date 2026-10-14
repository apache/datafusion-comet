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

-- COALESCE lowers to CASE WHEN with its last argument as ELSE, but must retain the first
-- argument's struct field names. Native to_json exposes names that row comparison ignores.
-- https://github.com/apache/datafusion-comet/issues/6482
-- Config: spark.sql.caseSensitive=false
-- Config: spark.comet.expression.StructsToJson.allowIncompatible=true

statement
CREATE TABLE test_coalesce_field_names(p boolean, q boolean, i int) USING parquet

statement
INSERT INTO test_coalesce_field_names VALUES
  (true, false, 1), (false, true, 2), (NULL, false, 3), (false, NULL, NULL)

query expect_native(coalesce)
SELECT to_json(coalesce(
  CASE WHEN p THEN named_struct('x', i) END,
  CASE WHEN q THEN named_struct('X', i + 100) END,
  named_struct('X', 7)))
FROM test_coalesce_field_names

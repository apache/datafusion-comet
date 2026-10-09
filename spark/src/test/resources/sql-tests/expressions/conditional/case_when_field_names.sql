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

-- Spark preserves the first THEN's struct field names when branches differ only in case.
-- Native to_json exposes Arrow field names that the row comparison would otherwise ignore.
-- https://github.com/apache/datafusion-comet/issues/6482
-- Config: spark.sql.caseSensitive=false
-- Config: spark.comet.expression.StructsToJson.allowIncompatible=true

statement
CREATE TABLE test_case_field_names(p boolean, q boolean, i int, d double) USING parquet

statement
INSERT INTO test_case_field_names VALUES
  (true, false, 1, 1.5), (false, true, 2, 2.5),
  (NULL, false, 3, 3.5), (false, NULL, NULL, NULL)

-- The result keeps the THEN name for both selected branches, in either name order.
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN p THEN named_struct('x', i) ELSE named_struct('X', i) END),
  to_json(CASE WHEN p THEN named_struct('X', i) ELSE named_struct('x', i) END)
FROM test_case_field_names

-- Later THEN branches and an omitted ELSE must also preserve the first THEN's name.
query expect_native(casewhen)
SELECT
  to_json(CASE WHEN p THEN named_struct('x', i)
    WHEN q THEN named_struct('X', i + 10) ELSE named_struct('X', i + 20) END),
  to_json(CASE WHEN p THEN named_struct('x', i)
    WHEN q THEN named_struct('X', i + 10) END)
FROM test_case_field_names

-- Fields align by position. Matching these names would incorrectly combine INT with DOUBLE.
query expect_native(casewhen)
SELECT to_json(CASE WHEN p THEN named_struct('x', i, 'X', CAST(5.5 AS DOUBLE))
  ELSE named_struct('X', 0, 'x', d) END)
FROM test_case_field_names

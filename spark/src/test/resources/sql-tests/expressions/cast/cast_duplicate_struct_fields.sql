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

statement
CREATE TABLE test_cast_duplicate_struct_fields(s STRUCT<p: INT, q: INT>) USING parquet

statement
INSERT INTO test_cast_duplicate_struct_fields VALUES
  (named_struct('p', 1, 'q', 11)),
  (named_struct('p', 2, 'q', 12))

query expect_fallback(Cast target contains a struct with duplicate field names)
SELECT CAST(s AS STRUCT<x: INT, x: INT>) FROM test_cast_duplicate_struct_fields

query expect_fallback(Cast target contains a struct with duplicate field names)
SELECT CAST(array(s) AS ARRAY<STRUCT<x: INT, x: INT>>)
FROM test_cast_duplicate_struct_fields

query expect_fallback(Cast target contains a struct with duplicate field names)
SELECT CAST(map('key', s) AS MAP<STRING, STRUCT<x: INT, x: INT>>)
FROM test_cast_duplicate_struct_fields

-- Distinct field names remain on the native path.
query
SELECT CAST(s AS STRUCT<x: INT, y: INT>) FROM test_cast_duplicate_struct_fields

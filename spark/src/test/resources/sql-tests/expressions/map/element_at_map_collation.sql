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

-- Spark 4.0+ supports string collations. Spark compares a map key under its declared collation,
-- so a `UTF8_LCASE` map matches `A1` against a dynamic `a1` lookup and returns `7`. Comet's native
-- `map_extract` compares string keys as `UTF8_BINARY`, so `CometElementAt` declines the lookup and
-- Spark evaluates the projection. The `element_at` form is used rather than `m[key]` because
-- Spark's `SimplifyExtractValueOps` rewrites `map(...)[key]` over a literal map into a `CASE`
-- before it can reach the native map lookup.

statement
CREATE TABLE test_element_at_collation(k string) USING parquet

statement
INSERT INTO test_element_at_collation VALUES ('a1'), ('A1'), ('zz'), (NULL)

query expect_fallback(cannot honour a non-default collation)
SELECT element_at(map(CAST('A1' AS STRING COLLATE UTF8_LCASE), 7), CAST(k AS STRING COLLATE UTF8_LCASE))
FROM test_element_at_collation

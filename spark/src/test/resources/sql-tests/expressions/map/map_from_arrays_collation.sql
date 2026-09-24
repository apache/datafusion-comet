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

-- Comet compares string keys as UTF8_BINARY, so a collated key type falls back to Spark

statement
CREATE TABLE test_map_from_arrays_collation(k1 string, k2 string, v int) USING parquet

statement
INSERT INTO test_map_from_arrays_collation VALUES ('a', 'B', 1), ('x', 'y', NULL)

query expect_fallback(non-default collation)
SELECT map_from_arrays(
  array(CAST(k1 AS STRING COLLATE UTF8_LCASE), CAST(k2 AS STRING COLLATE UTF8_LCASE)),
  array(v, v))
FROM test_map_from_arrays_collation

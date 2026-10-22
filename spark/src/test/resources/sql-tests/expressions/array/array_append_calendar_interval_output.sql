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

-- The nondeterministic nullable array forces the dispatcher, whose interval conversion overflows
-- past about 292 years (#5279), as the 1e10-second rows do, so the result falls back to Spark.
-- Spark 4.0 rewrites `array_append` to `array_insert(-1)`, so this is Spark 3.x only.

-- MaxSparkVersion: 3.5

statement
CREATE TABLE test_array_append_interval(s decimal(18, 6)) USING parquet

statement
INSERT INTO test_array_append_interval VALUES (10000000000.000000), (-10000000000.000000), (1.000000), (NULL)

query expect_fallback(holds a calendar interval)
SELECT array_append(
  IF(monotonically_increasing_id() >= 0, array(make_interval(0, 0, 0, 0, 0, 0, s)), NULL),
  make_interval(0, 0, 0, 0, 0, 0, s))
FROM test_array_append_interval

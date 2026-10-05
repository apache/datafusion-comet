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

-- A nondeterministic item has no native `array_append` path, but the JVM codegen dispatcher writes
-- a calendar interval's microseconds to Arrow as nanoseconds, which overflows a long beyond about
-- 292 years (https://github.com/apache/datafusion-comet/issues/5279). A result holding a calendar
-- interval therefore falls back to Spark rather than dispatching.
--
-- Spark 4.0 rewrites `array_append` to `array_insert(-1)` before serde, so `CometArrayAppend` is
-- only reachable on Spark 3.x.

-- MaxSparkVersion: 3.5

statement
CREATE TABLE test_array_append_interval(s decimal(18, 6)) USING parquet

statement
INSERT INTO test_array_append_interval VALUES (10000000000.000000), (-10000000000.000000), (1.000000), (NULL)

query expect_fallback(holds a calendar interval)
SELECT array_append(array(make_interval(0, 0, 0, 0, 0, 0, s)),
  IF(monotonically_increasing_id() >= 0, make_interval(0, 0, 0, 0, 0, 0, s), NULL))
FROM test_array_append_interval

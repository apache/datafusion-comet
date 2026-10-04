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

-- Regression for https://github.com/apache/datafusion-comet/issues/5532.
-- Both native decoding and JVM dispatch must preserve rows skipped by LIMIT.
-- ConfigMatrix: spark.comet.exec.scalaUDF.codegen.enabled=false,true
-- Config: spark.sql.shuffle.partitions=1

-- One writer preserves the VALUES order in a single Parquet file: the valid value precedes
-- the malformed suffix. This pins physical scan order, not SQL ordering without ORDER BY.
statement
CREATE TABLE test_unbase64_operator_limit USING parquet AS
SELECT /*+ COALESCE(1) */ bad FROM VALUES ('YWJj'), ('A') AS t(bad)

-- Spark stops after decoding 'abc' and never evaluates the malformed second row.
query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT hex(unbase64(bad)) FROM test_unbase64_operator_limit LIMIT 1

query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT bad FROM test_unbase64_operator_limit
WHERE unbase64(bad) <=> X'616263' LIMIT 1

-- A compound argument reaches the dispatcher, which also evaluates whole batches.
query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT hex(unbase64(concat(bad, ''))) FROM test_unbase64_operator_limit LIMIT 1

-- Consuming the malformed row must still raise the decoder error.
query expect_error(Last unit does not have enough valid bits)
SELECT hex(unbase64(bad)) FROM test_unbase64_operator_limit WHERE bad = 'A' LIMIT 1

-- Without an early-stop operator, decoding valid input retains the native kernel.
query expect_native(unbase64)
SELECT hex(unbase64(bad)) FROM test_unbase64_operator_limit WHERE bad = 'YWJj'

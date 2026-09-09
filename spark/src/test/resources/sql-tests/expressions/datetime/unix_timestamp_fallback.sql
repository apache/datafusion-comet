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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_unix_ts_fallback(s string, fmt string, d date, ts timestamp, ntz timestamp_ntz) USING parquet

statement
INSERT INTO test_unix_ts_fallback VALUES
  ('2024-06-15', 'yyyy-MM-dd', date('2024-06-15'), timestamp('2024-06-15 10:30:45'), CAST('2024-06-15 10:30:45' AS TIMESTAMP_NTZ)),
  (NULL, NULL, NULL, NULL, NULL)

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT unix_timestamp(s) FROM test_unix_ts_fallback

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT unix_timestamp(s, fmt) FROM test_unix_ts_fallback

query expect_fallback(spark.comet.exec.scalaUDF.codegen.enabled)
SELECT unix_timestamp('2024-06-15', 'yyyy-MM-dd')

-- Date and timestamp inputs keep their native path and ignore the format.
query
SELECT unix_timestamp(d), unix_timestamp(ts), unix_timestamp(ntz) FROM test_unix_ts_fallback

query
SELECT unix_timestamp(d, fmt), unix_timestamp(ts, fmt), unix_timestamp(ntz, fmt) FROM test_unix_ts_fallback

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

-- ANSI mode: cast(string as date) must raise CAST_INVALID_INPUT for a string that has the
-- shape of a date but names no real calendar date, rather than returning NULL.
-- See https://github.com/apache/datafusion-comet/issues/5012.
--
-- Per-string parity with Spark is covered by CometCastSuite "cast StringType to DateType";
-- this fixture covers the plan shapes that suite does not exercise.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_ansi_date_str(s string) USING parquet

statement
INSERT INTO test_ansi_date_str VALUES ('2016-13-01'), ('2016-02-30'), ('2016-01-01')

-- cast over a column projection
query expect_error(CAST_INVALID_INPUT)
SELECT cast(s as date) FROM test_ansi_date_str

-- cast inside a filter predicate must not silently drop the rows Spark fails on
query expect_error(CAST_INVALID_INPUT)
SELECT count(*) FROM test_ansi_date_str WHERE cast(s as date) > date'2000-01-01'

-- to_date routes through the same cast
query expect_error(CAST_INVALID_INPUT)
SELECT to_date(s) FROM test_ansi_date_str

-- try_cast still returns NULL under ANSI, and valid input still casts. These non-error
-- queries are the sentinel proving the casts above ran natively rather than falling back
-- to Spark, which would make every expect_error above pass vacuously.
query
SELECT try_cast(s as date) FROM test_ansi_date_str

query
SELECT cast(s as date) FROM test_ansi_date_str WHERE s = '2016-01-01'

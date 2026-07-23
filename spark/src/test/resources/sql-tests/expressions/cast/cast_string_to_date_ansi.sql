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

-- ANSI mode: cast(string as date) must raise CAST_INVALID_INPUT for malformed input.
-- See https://github.com/apache/datafusion-comet/issues/5012 for the case where a string
-- with the shape of a date but no matching calendar date returned NULL instead.

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Structurally malformed strings
-- ============================================================================

query expect_error(CAST_INVALID_INPUT)
SELECT cast('abc' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2020-010-01' as date)

-- ============================================================================
-- Canonical yyyy-mm-dd shape that is not a real calendar date
-- ============================================================================

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2016-13-01' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2016-02-30' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2016-01-32' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2021-02-29' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2020-00-15' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2020-01-00' as date)

-- ============================================================================
-- Same invalid calendar dates in non-canonical shapes
-- ============================================================================

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2020-2-30' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2020-13-1' as date)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('2020-02-30T' as date)

-- to_date goes through the same cast
query expect_error(CAST_INVALID_INPUT)
SELECT to_date('2016-13-01')

-- ============================================================================
-- Filtering on the cast must not silently drop rows Spark fails on
-- ============================================================================

statement
CREATE TABLE test_ansi_date_str(s string) USING parquet

statement
INSERT INTO test_ansi_date_str VALUES ('2016-13-01'), ('2016-02-30'), ('2016-01-01')

query expect_error(CAST_INVALID_INPUT)
SELECT cast(s as date) FROM test_ansi_date_str

query expect_error(CAST_INVALID_INPUT)
SELECT count(*) FROM test_ansi_date_str WHERE cast(s as date) > date'2000-01-01'

-- ============================================================================
-- try_cast still returns NULL under ANSI, and valid input still casts.
-- These non-error queries are the sentinel proving the casts above ran natively
-- rather than falling back to Spark.
-- ============================================================================

query
SELECT try_cast(s as date) FROM test_ansi_date_str

query
SELECT cast('2016-01-01' as date), cast('2020-02-29' as date), cast('2020-1-1' as date)

query
SELECT cast('262142-01-01' as date), cast('-262143-12-31' as date)

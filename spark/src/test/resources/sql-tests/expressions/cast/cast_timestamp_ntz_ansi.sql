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

-- Config: spark.sql.ansi.enabled=true
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

-- ANSI mode: invalid String -> NTZ should error
query expect_error(CAST_INVALID_INPUT)
SELECT cast('invalid' as timestamp_ntz)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('T12:34:56' as timestamp_ntz)

query expect_error(CAST_INVALID_INPUT)
SELECT cast('' as timestamp_ntz)

-- ANSI mode: parseable but invalid date should error, not return NULL
query expect_error(CAST_INVALID_INPUT)
SELECT cast('2023-02-29' as timestamp_ntz)

-- TRY_CAST: returns NULL even in ANSI mode
-- IgnoreFromSparkVersion: 4.1 https://github.com/apache/datafusion-comet/issues/4098
query
SELECT try_cast('invalid' as timestamp_ntz), try_cast('T12:34' as timestamp_ntz)

-- TRY_CAST: valid inputs still work
query
SELECT try_cast('2020-01-01 12:34:56' as timestamp_ntz)

-- Valid casts should still work in ANSI mode
query
SELECT cast('2020-01-01 12:34:56.123456' as timestamp_ntz)

query
SELECT cast('2020-06-15 12:30:00Z' as timestamp_ntz)

query
SELECT cast('2024-03-10 02:30:00' as timestamp_ntz)

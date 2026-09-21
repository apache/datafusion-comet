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

-- hour/minute/second dispatch on the session timezone: with a zero-offset session the value is
-- read straight from the stored microseconds, and otherwise it is shifted to the session zone
-- first. Pinning both zones exercises both branches rather than leaving the choice to the
-- ambient JVM zone of whichever runner executes this.
-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles

statement
CREATE TABLE test_minute(ts timestamp) USING parquet

statement
INSERT INTO test_minute VALUES (timestamp('2024-01-15 10:00:00')), (timestamp('2024-01-15 10:30:00')), (timestamp('2024-01-15 10:59:59')), (timestamp('1969-12-31 23:59:59')), (NULL)

query
SELECT minute(ts) FROM test_minute

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3336)
SELECT minute(timestamp('2024-01-15 10:00:00')), minute(timestamp('2024-01-15 10:30:00')), minute(timestamp('2024-01-15 10:59:59'))

-- TimestampNTZ: the native impl is Incompatible for TimestampNTZType (#3180), so this is routed
-- through the codegen dispatcher and stays native while matching Spark.
statement
CREATE TABLE test_minute_ntz(ts timestamp_ntz) USING parquet

statement
INSERT INTO test_minute_ntz VALUES (cast('2024-01-15 00:00:00' as timestamp_ntz)), (cast('2024-01-15 12:30:45' as timestamp_ntz)), (cast('2024-01-15 23:59:59' as timestamp_ntz)), (cast('1969-12-31 23:59:59' as timestamp_ntz)), (NULL)

query
SELECT minute(ts) FROM test_minute_ntz

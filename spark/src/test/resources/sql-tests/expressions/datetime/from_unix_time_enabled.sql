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

-- Test from_unixtime() with allowIncompatible enabled (happy path)
-- Uses UTC timezone to ensure results match Spark
-- Config: spark.comet.expression.FromUnixTime.allowIncompatible=true
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_from_unix_time_enabled(t long) USING parquet

statement
INSERT INTO test_from_unix_time_enabled VALUES (0), (1718451045), (-1), (NULL), (2147483647)

-- With allowIncompatible=true, the default datetime pattern runs natively
query
SELECT from_unixtime(t) FROM test_from_unix_time_enabled

-- Non-default format patterns are handled by the JVM codegen dispatcher
query
SELECT from_unixtime(t, 'yyyy-MM-dd') FROM test_from_unix_time_enabled

-- literal arguments
query
SELECT from_unixtime(0)

query
SELECT from_unixtime(1718451045, 'yyyy-MM-dd')

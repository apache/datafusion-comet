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

-- Companion to trunc_timestamp.sql exercising the native (Rust) date_trunc implementation with a
-- non-literal format. A non-literal format is Incompatible by default because the native impl
-- throws on an invalid format string instead of returning NULL like Spark, so it is routed
-- through the codegen dispatcher. With allowIncompatible enabled the native UDF runs instead. With
-- a UTC session timezone and only valid format values, the native results match Spark.

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_trunc_ts_fmt(ts timestamp, fmt string) USING parquet

statement
INSERT INTO test_trunc_ts_fmt VALUES (timestamp('2024-06-15 10:30:45'), 'year'), (timestamp('2024-06-15 10:30:45'), 'month'), (timestamp('2024-06-15 10:30:45'), 'day'), (timestamp('2024-06-15 10:30:45'), 'hour'), (NULL, 'year')

query
SELECT date_trunc(fmt, ts) FROM test_trunc_ts_fmt

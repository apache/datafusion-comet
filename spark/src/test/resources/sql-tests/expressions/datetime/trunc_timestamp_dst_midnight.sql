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

-- DST transitions at midnight. America/Sao_Paulo skipped midnight on 2018-11-04,
-- so DAY truncation and the day's start fall in the gap, and 23:00-24:00 on
-- 2019-02-16 happened twice, once at -02:00 and once at -03:00.
-- https://github.com/apache/datafusion-comet/issues/5633

-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=true
-- Config: spark.sql.session.timeZone=America/Sao_Paulo

statement
CREATE TABLE test_trunc_dst_midnight(ts timestamp) USING parquet

statement
INSERT INTO test_trunc_dst_midnight VALUES
  (timestamp('2018-11-03 23:30:00-03:00')),
  (timestamp('2018-11-04 01:30:00-02:00')),
  (timestamp('2018-11-04 13:00:00-02:00')),
  (timestamp('2019-02-16 23:30:00-02:00')),
  (timestamp('2019-02-16 23:30:00-03:00')),
  (timestamp('2019-02-17 00:30:00-03:00')),
  (NULL)

query
SELECT ts, date_trunc('MINUTE', ts), date_trunc('HOUR', ts), date_trunc('DAY', ts) FROM test_trunc_dst_midnight ORDER BY ts

query
SELECT ts, date_trunc('WEEK', ts), date_trunc('MONTH', ts) FROM test_trunc_dst_midnight ORDER BY ts

query
SELECT ts, date_trunc('QUARTER', ts), date_trunc('YEAR', ts) FROM test_trunc_dst_midnight ORDER BY ts

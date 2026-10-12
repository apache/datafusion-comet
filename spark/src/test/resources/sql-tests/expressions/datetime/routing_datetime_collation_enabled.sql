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

-- MinSparkVersion: 4.0
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.expression.NextDay.allowIncompatible=false
-- Config: spark.comet.expression.TruncDate.allowIncompatible=false
-- Config: spark.comet.expression.TruncTimestamp.allowIncompatible=false
-- Config: spark.comet.expression.DateFormatClass.allowIncompatible=false
-- Config: spark.comet.expression.ConvertTimezone.allowIncompatible=false
-- Config: spark.comet.expression.FromUnixTime.allowIncompatible=false
-- Config: spark.comet.expression.MakeTimestamp.allowIncompatible=false
-- Config: spark.comet.expression.ToUnixTimestamp.allowIncompatible=false

statement
CREATE TABLE routing_datetime_collation(ts TIMESTAMP, d DATE, fmt STRING, day STRING, zone STRING, str_ts STRING, seconds BIGINT) USING parquet

statement
INSERT INTO routing_datetime_collation VALUES (TIMESTAMP '2024-06-15 12:34:56', DATE '2024-06-15', 'YEAR', 'MON', 'UTC', '2024-06-15', 0), (NULL, NULL, NULL, NULL, NULL, NULL, NULL)

query expect_native(next_day)
SELECT next_day(d, day) FROM routing_datetime_collation

query expect_dispatch(next_day)
SELECT next_day(d, day COLLATE UTF8_LCASE) FROM routing_datetime_collation

query expect_dispatch(trunc)
SELECT trunc(d, fmt COLLATE UTF8_LCASE) FROM routing_datetime_collation

query expect_dispatch(date_trunc)
SELECT date_trunc(fmt COLLATE UTF8_LCASE, ts) FROM routing_datetime_collation

query expect_dispatch(date_format)
SELECT date_format(ts, 'yyyy-MM-dd' COLLATE UTF8_LCASE) FROM routing_datetime_collation

query expect_dispatch(from_unixtime)
SELECT from_unixtime(seconds, 'yyyy-MM-dd' COLLATE UTF8_LCASE) FROM routing_datetime_collation

query expect_dispatch(convert_timezone)
SELECT convert_timezone(zone COLLATE UTF8_LCASE, 'America/Los_Angeles', CAST(ts AS TIMESTAMP_NTZ)) FROM routing_datetime_collation

query expect_dispatch(make_timestamp)
SELECT make_timestamp(2024, 6, 15, 12, 0, 0, zone COLLATE UTF8_LCASE) FROM routing_datetime_collation

query expect_dispatch(to_unix_timestamp)
SELECT to_unix_timestamp(str_ts, 'yyyy-MM-dd' COLLATE UTF8_LCASE) FROM routing_datetime_collation

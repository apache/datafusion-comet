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

statement
CREATE TABLE test_abs(i int, l long, f float, d double) USING parquet

statement
INSERT INTO test_abs VALUES (1, 1, 1.5, 1.5), (-1, -1, -1.5, -1.5), (0, 0, 0.0, 0.0), (NULL, NULL, NULL, NULL), (2147483647, 9223372036854775807, cast('Infinity' as float), cast('NaN' as double))

query
SELECT abs(i), abs(l), abs(f), abs(d) FROM test_abs

-- literal arguments
query
SELECT abs(-5), abs(-1.5), abs(0), abs(NULL)

-- abs() on intervals has no native impl; routed through the JVM codegen dispatcher.
-- Interval values are built inline: native Parquet scan of interval columns is unsupported
-- (https://github.com/apache/datafusion-comet/issues/5060), and a top-level YearMonthIntervalType projection column is still rejected by the
-- projection type gate (https://github.com/apache/datafusion-comet/issues/5061), so the ym result is wrapped in a struct.
query
SELECT abs(make_dt_interval(1, 2, 3, 4.5)) AS dt_pos,
       abs(make_dt_interval(-1, -2, -3, -4.5)) AS dt_neg,
       abs(make_dt_interval(0, 0, 0, 0)) AS dt_zero,
       abs(CAST(NULL AS INTERVAL DAY TO SECOND)) AS dt_null

-- interval year to month: dispatched the same way; wrapped in a struct because a top-level
-- YearMonthIntervalType column is rejected by the projection output type gate (https://github.com/apache/datafusion-comet/issues/5061)
query
SELECT named_struct('v', abs(make_ym_interval(1, 6))) AS ym_pos,
       named_struct('v', abs(make_ym_interval(-1, -6))) AS ym_neg

query expect_error(overflow)
SELECT abs(make_dt_interval(-106751991, -4, 0, -54.775808))

query expect_error(overflow)
SELECT abs(make_ym_interval(0, -2147483648))

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

-- Native ANSI execution must preserve Spark's overflow exception.
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.expression.MakeInterval.allowIncompatible=true

statement
CREATE TABLE test_make_interval_ansi(years int) USING parquet

statement
INSERT INTO test_make_interval_ansi VALUES (NULL)

query
SELECT make_interval(1, 2, 3, 4, 5, 6, 7.123456)

query
SELECT make_interval(years) FROM test_make_interval_ansi

query expect_error(overflow)
SELECT make_interval(2147483647)

query expect_error(overflow)
SELECT make_interval(0, 0, 2147483647)

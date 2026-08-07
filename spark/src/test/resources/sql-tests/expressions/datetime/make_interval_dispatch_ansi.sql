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

statement
CREATE TABLE test_make_interval_dispatch_ansi(years int, weeks int, hours int) USING parquet

statement
INSERT INTO test_make_interval_dispatch_ansi VALUES
  (1, 0, 5),
  (2147483647, 0, 0),
  (0, 2147483647, 0),
  (0, 0, 2562048)

query
SELECT make_interval(years, 0, weeks, 0, hours)
FROM test_make_interval_dispatch_ansi
WHERE years = 1

query expect_error(overflow. If necessary set)
SELECT make_interval(years)
FROM test_make_interval_dispatch_ansi
WHERE years = 2147483647

query expect_error(overflow. If necessary set)
SELECT make_interval(0, 0, weeks)
FROM test_make_interval_dispatch_ansi
WHERE weeks = 2147483647

query
SELECT make_interval(0, 0, 0, 0, hours)
FROM test_make_interval_dispatch_ansi
WHERE hours = 2562048

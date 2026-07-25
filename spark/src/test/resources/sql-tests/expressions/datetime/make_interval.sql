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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE test_make_interval(
  years int,
  months int,
  weeks int,
  days int,
  hours int,
  mins int,
  secs decimal(38, 6)) USING parquet

statement
INSERT INTO test_make_interval VALUES
  (1, 2, 3, 4, 5, 6, 7.123456),
  (0, 1, 0, 1, 0, 0, 100.000001),
  (-1, -2, -1, -1, -1, -1, -1.500000),
  (NULL, 1, 2, 3, 4, 5, 6.000000)

query
SELECT make_interval(years, months, weeks, days, hours, mins, secs)
FROM test_make_interval
ORDER BY years

query
SELECT make_interval(1, 2), make_interval(3), make_interval()

query
SELECT make_interval(2147483647)

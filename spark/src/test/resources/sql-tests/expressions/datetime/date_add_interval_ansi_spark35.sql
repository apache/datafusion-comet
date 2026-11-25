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

-- With ANSI on, day-granular intervals stay on the date path and any time part is rejected.
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- MaxSparkVersion: 3.5

statement
CREATE TABLE test_date_add_interval_ansi(d date, m int, dd int) USING parquet

statement
INSERT INTO test_date_add_interval_ansi VALUES
  (date'2024-01-31', 1, 1),
  (date'2024-02-29', 12, 0),
  (date'2024-03-31', -1, -1),
  (date'2024-06-15', NULL, 1),
  (NULL, 1, 1)

-- the NULL month row builds a NULL interval in the kernel and yields NULL, not an error; a NULL
-- literal would be folded
query expect_dispatch(dateaddinterval)
SELECT d, m, dd, d + make_interval(0, m, 0, dd), d - make_interval(0, 1, 0, 1)
FROM test_date_add_interval_ansi

query expect_error(Cannot add hours, minutes or seconds, milliseconds, microseconds to a date)
SELECT d + make_interval(0, 1, 0, 1, 12) FROM test_date_add_interval_ansi

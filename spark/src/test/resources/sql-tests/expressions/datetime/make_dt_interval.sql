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

-- Config: spark.comet.expression.MakeDTInterval.allowIncompatible=true

-- literal arguments, all arities (omitted args default to 0)
query
SELECT make_dt_interval()

query
SELECT make_dt_interval(1)

query
SELECT make_dt_interval(1, 2)

query
SELECT make_dt_interval(1, 2, 3)

query
SELECT make_dt_interval(1, 2, 3, 4.5)

-- microsecond precision in the seconds component
query
SELECT make_dt_interval(0, 0, 0, 4.123456)

-- negative components
query
SELECT make_dt_interval(-1, -2, -3, -4.5)

-- null-intolerant: any null input yields a null result
query
SELECT make_dt_interval(CAST(NULL AS INT), 2, 3, 4.5)

query
SELECT make_dt_interval(1, 2, 3, CAST(NULL AS DECIMAL(18, 6)))

-- column inputs (not constant-folded): exercises the per-row path
statement
CREATE TABLE test_make_dt_interval(id INT, d INT, h INT, m INT, s DECIMAL(18, 6)) USING parquet

statement
INSERT INTO test_make_dt_interval VALUES
  (1, 1, 2, 3, 4.5),
  (2, 0, 0, 0, 0),
  (3, -1, -2, -3, -4.123456),
  (4, 100, 23, 59, 59.999999),
  (5, NULL, 2, 3, 4.5)

query
SELECT id, make_dt_interval(d, h, m, s) FROM test_make_dt_interval

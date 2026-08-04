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

-- MakeInterval.failOnError defaults to SQLConf.get.ansiEnabled, so in ANSI mode overflow
-- raises ARITHMETIC_OVERFLOW instead of returning NULL. The exception has to cross out of
-- the generated kernel and surface as the same Spark error.
-- See make_interval.sql for the non-ANSI coverage.
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_make_interval_ansi(y int, mo int, w int, d int, h int, mi int, s decimal(18,6)) USING parquet

statement
INSERT INTO test_make_interval_ansi VALUES
  (1, 2, 3, 4, 5, 6, 7.008009),
  (-1, -2, -3, -4, -5, -6, -7.500000),
  (NULL, 2, 3, 4, 5, 6, 7.008009)

-- valid inputs still evaluate normally under ANSI, and act as the sentinel proving the
-- expression is not silently falling back to Spark
query
SELECT make_interval(y, mo, w, d, h, mi, s) FROM test_make_interval_ansi

query
SELECT make_interval(1, 2, 3, 4, 5, 6, 7.008009)

-- years * 12 overflows the int range. Spark 3.5 renders ARITHMETIC_OVERFLOW without the
-- condition name in the message, so match on the shared "overflow" text instead.
query expect_error(overflow)
SELECT make_interval(200000000, 0, 0, 0, 0, 0, 0)

-- weeks * 7 overflows the int range
query expect_error(overflow)
SELECT make_interval(0, 0, 400000000, 0, 0, 0, 0)

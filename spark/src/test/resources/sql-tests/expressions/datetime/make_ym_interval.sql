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

-- Routes make_ym_interval through the codegen dispatcher; produces YearMonthIntervalType.

statement
CREATE TABLE test_myi(y int, m int) USING parquet

statement
INSERT INTO test_myi VALUES (1, 2), (0, 0), (-1, 6), (5, -3), (NULL, 3), (2, NULL)

query
SELECT y, m, make_ym_interval(y, m) FROM test_myi

-- literal arguments
query
SELECT make_ym_interval(1, 2), make_ym_interval(0, 0), make_ym_interval(-5, 11)

-- default arguments
query
SELECT make_ym_interval(3), make_ym_interval()

-- overflow: years * 12 exceeds Int range. makeYearMonthInterval throws unconditionally (not
-- ANSI-gated); this confirms the dispatched codegen path propagates Spark's exception. The
-- pattern is the lowercase word so it matches every version: Spark 4.x raises
-- INTERVAL_ARITHMETIC_OVERFLOW ("Integer overflow while operating with intervals") while Spark 3.x
-- raises a raw ArithmeticException ("integer overflow").
query expect_error(overflow)
SELECT make_ym_interval(2147483647)

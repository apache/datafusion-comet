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

-- ANSI mode pmod tests
-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_pmod_ansi(a int, b int) USING parquet

statement
INSERT INTO test_pmod_ansi VALUES (10, 0), (10, 3), (-7, 3)

-- pmod by zero should throw in ANSI mode
query expect_error(BY_ZERO)
SELECT pmod(a, b) FROM test_pmod_ansi WHERE b = 0

-- pmod with non-zero divisor should still work
query
SELECT pmod(a, b) FROM test_pmod_ansi WHERE b != 0

-- literal pmod by zero should throw
query expect_error(BY_ZERO)
SELECT pmod(10, 0)

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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_pmod_int(a int, b int) USING parquet

statement
INSERT INTO test_pmod_int VALUES (10, 3), (7, -2), (-7, 2), (-7, -2), (0, 5), (10, 0), (NULL, 3), (10, NULL)

-- integer column input
query
SELECT pmod(a, b) FROM test_pmod_int

-- integer literal arguments
query
SELECT pmod(10, 3), pmod(7, -2), pmod(-7, 2), pmod(-7, -2), pmod(10, 0), pmod(NULL, 3)

statement
CREATE TABLE test_pmod_double(a double, b double) USING parquet

statement
INSERT INTO test_pmod_double VALUES (10.5, 3.0), (-7.5, 2.0), (10.0, 0.0), (NULL, 1.0)

-- floating-point column input
query
SELECT pmod(a, b) FROM test_pmod_double

-- floating-point literal arguments
query
SELECT pmod(10.5, 3.0), pmod(-7.5, 2.0), pmod(10.0, 0.0)

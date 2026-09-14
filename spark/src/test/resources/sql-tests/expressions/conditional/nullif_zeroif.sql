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

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_nullif_zeroif(v int) USING parquet

statement
INSERT INTO test_nullif_zeroif VALUES (0), (1), (42), (-5), (NULL)

-- nullifzero: returns NULL when input is 0, otherwise returns input
query
SELECT nullifzero(v) FROM test_nullif_zeroif

-- zeroifnull: returns 0 when input is NULL, otherwise returns input
query
SELECT zeroifnull(v) FROM test_nullif_zeroif

-- literal arguments for nullifzero
query
SELECT nullifzero(0), nullifzero(1), nullifzero(-5), nullifzero(NULL)

-- literal arguments for zeroifnull
query
SELECT zeroifnull(0), zeroifnull(1), zeroifnull(-5), zeroifnull(NULL)

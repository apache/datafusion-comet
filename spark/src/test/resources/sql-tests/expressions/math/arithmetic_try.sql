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

-- TRY mode arithmetic tests for Remainder (try_mod)
-- try_mod returns NULL on divide-by-zero instead of throwing

-- MinSparkVersion: 4.0

statement
CREATE TABLE try_mod_int(a int, b int) USING parquet

statement
INSERT INTO try_mod_int VALUES (10, 3), (10, 0), (0, 0), (NULL, 1), (10, NULL), (-10, 3), (10, -3)

query
SELECT try_mod(a, b) FROM try_mod_int

query
SELECT try_mod(10, 3)

query
SELECT try_mod(10, 0)

query
SELECT try_mod(NULL, 1)

query
SELECT try_mod(10, NULL)

statement
CREATE TABLE try_mod_long(a long, b long) USING parquet

statement
INSERT INTO try_mod_long VALUES (10L, 3L), (10L, 0L), (0L, 0L), (NULL, 1L), (10L, NULL)

query
SELECT try_mod(a, b) FROM try_mod_long

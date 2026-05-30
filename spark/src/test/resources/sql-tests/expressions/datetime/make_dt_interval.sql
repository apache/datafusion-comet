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

-- Routes make_dt_interval through the codegen dispatcher; produces DayTimeIntervalType.

statement
CREATE TABLE test_mdi(d int, h int, mi int, s decimal(18,6)) USING parquet

statement
INSERT INTO test_mdi VALUES (1, 2, 3, 4.5), (0, 0, 0, 0), (-1, 0, 30, 15.250), (NULL, 1, 1, 1)

query
SELECT d, h, mi, s, make_dt_interval(d, h, mi, s) FROM test_mdi

-- literal arguments
query
SELECT make_dt_interval(1, 2, 3, 4.5), make_dt_interval(0, 0, 0, 0)

-- default arguments
query
SELECT make_dt_interval(1), make_dt_interval(1, 2), make_dt_interval()

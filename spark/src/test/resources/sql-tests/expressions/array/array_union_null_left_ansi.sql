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

-- Config: spark.sql.ansi.enabled=true

-- Spark skips the right operand of array_union on rows whose left array is NULL, so an ANSI cast
-- there never runs. https://github.com/apache/datafusion-comet/issues/6613

statement
CREATE TABLE array_union_ansi(a array<int>, s string) USING parquet

statement
INSERT INTO array_union_ansi VALUES (NULL, 'bad'), (array(1), '1')

query
SELECT array_union(a, array(CAST(s AS INT))) FROM array_union_ansi

statement
CREATE TABLE array_union_ansi_bad(a array<int>, s string) USING parquet

statement
INSERT INTO array_union_ansi_bad VALUES (array(1), 'bad')

-- Where the array isn't NULL, both evaluate the cast and fail
query expect_error(CAST_INVALID_INPUT)
SELECT array_union(a, array(CAST(s AS INT))) FROM array_union_ansi_bad

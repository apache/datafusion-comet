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

-- Spark skips array_contains' value when the array is null, so a value that throws is not
-- evaluated for that row. The native kernel evaluates both arguments first, so a value other than
-- a literal or a column read stays on the codegen dispatcher when the array can be null.
-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE ac_eager(a ARRAY<DOUBLE>, s STRING) USING parquet

statement
INSERT INTO ac_eager VALUES (NULL, 'bad'), (array(1.0D), '1')

query expect_dispatch(array_contains)
SELECT array_contains(a, CAST(s AS DOUBLE)) FROM ac_eager

-- A column read is safe to evaluate early, so it stays native
query expect_native(array_contains)
SELECT array_contains(a, 1.0D), array_contains(a, CAST(NULL AS DOUBLE)) FROM ac_eager

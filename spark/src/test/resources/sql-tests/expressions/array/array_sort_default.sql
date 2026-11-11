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
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- The default comparator is native even when the JVM codegen dispatcher is disabled. Null
-- elements sort last, while null fields and elements inside a non-null value sort first.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=false

statement
CREATE TABLE array_sort_default_nested (id INT, a ARRAY<ARRAY<INT>>) USING parquet

statement
INSERT INTO array_sort_default_nested VALUES
  (1, array(array(1), array(CAST(NULL AS INT)), CAST(NULL AS ARRAY<INT>), array(0))),
  (2, CAST(NULL AS ARRAY<ARRAY<INT>>)),
  (3, CAST(array() AS ARRAY<ARRAY<INT>>)),
  (4, CAST(array(NULL, NULL) AS ARRAY<ARRAY<INT>>))

query expect_native(array_sort)
SELECT id, array_sort(a) FROM array_sort_default_nested ORDER BY id

statement
CREATE TABLE array_sort_default_types (
  i INT, s STRING, b BINARY, d DECIMAL(10, 2), day DATE, ts TIMESTAMP
) USING parquet

statement
INSERT INTO array_sort_default_types VALUES
  (2, 'b', X'02', 2.00, DATE '2024-01-02', TIMESTAMP '2024-01-02 12:00:00'),
  (NULL, 'a', X'01', 1.00, DATE '2024-01-01', TIMESTAMP '2024-01-01 12:00:00')

-- `coalesce(i, 0)` makes both array elements non-nullable, reaching the noNulls comparator.
query expect_native(array_sort)
SELECT array_sort(array(coalesce(i, 0), 1)) FROM array_sort_default_types

query expect_native(array_sort)
SELECT array_sort(array(s, 'a', CAST(NULL AS STRING))) FROM array_sort_default_types

query expect_native(array_sort)
SELECT array_sort(array(b, X'01', CAST(NULL AS BINARY))) FROM array_sort_default_types

query expect_native(array_sort)
SELECT array_sort(array(d, CAST(1 AS DECIMAL(10, 2)), CAST(NULL AS DECIMAL(10, 2))))
FROM array_sort_default_types

query expect_native(array_sort)
SELECT array_sort(array(day, DATE '2024-01-01', CAST(NULL AS DATE)))
FROM array_sort_default_types

query expect_native(array_sort)
SELECT array_sort(array(ts, TIMESTAMP '2024-01-01 12:00:00', CAST(NULL AS TIMESTAMP)))
FROM array_sort_default_types

query expect_native(array_sort)
SELECT array_sort(array(named_struct('n', i), named_struct('n', 1),
                        CAST(NULL AS STRUCT<n:INT>)))
FROM array_sort_default_types

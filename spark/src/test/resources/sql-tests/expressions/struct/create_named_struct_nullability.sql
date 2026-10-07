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

-- Config: spark.comet.exec.range.enabled=true
-- Config: spark.sql.caseSensitive=false

-- Catalyst proves both fields nonnullable, but native cast inference is conservative.
-- array_repeat preserves the struct type, so array_union needs the same field flags on both
-- sides. Unlike array(...), this path does not cast elements to a deeply-nullable type.
query expect_native(array_union)
SELECT array_union(
  array_repeat(named_struct('x', CAST(id AS INT)), 2),
  array_repeat(named_struct('x', IF(id = 0, 0, 1)), 2))
FROM range(3)

-- Preserve the field flags recursively through another struct constructor.
query expect_native(array_union)
SELECT array_union(
  array_repeat(named_struct('inner', named_struct('x', CAST(id AS INT))), 2),
  array_repeat(named_struct('inner', named_struct('x', IF(id = 0, 0, 1))), 2))
FROM range(3)

-- Map construction preserves Catalyst's nested nullability. CASE must union those flags by
-- position, even when case-insensitive field names swap places, so neither NULL field is lost.
query
SELECT CASE WHEN id % 2 = 0
  THEN map(1, named_struct('x', CAST(id AS DOUBLE), 'X', CAST(NULL AS DOUBLE)))
  ELSE map(1, named_struct('X', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE)))
END FROM range(4)

-- An untyped NULL branch must not restore name-based struct coercion.
query
SELECT CASE WHEN id % 3 = 0
  THEN map(1, named_struct('x', CAST(id AS DOUBLE), 'X', CAST(NULL AS DOUBLE)))
  WHEN id % 3 = 1 THEN NULL
  ELSE map(1, named_struct('X', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE)))
END FROM range(6)

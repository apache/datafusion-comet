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

-- Config: spark.comet.sparkToColumnar.enabled=true
-- Config: spark.comet.sparkToColumnar.supportedOperatorList=Range
-- Config: spark.sql.caseSensitive=false

-- Catalyst proves BIGINT -> DOUBLE nonnullable; native cast inference is conservative.
-- The computed struct must have the same field flags as the typed NULL array element.
query
SELECT array(named_struct('score', CAST(id AS DOUBLE),
                          'amount', CAST(id AS DECIMAL(18,2))), NULL)
FROM range(8)

-- The field contract must also survive a nested constructor with an actually nullable child.
query
SELECT array(named_struct('nested', named_struct('score', CAST(id AS DOUBLE),
                         'optional', CASE WHEN id % 2 = 0 THEN id END)), NULL)
FROM range(8)

-- CASE unions nullability by ordinal, even when case-insensitive names swap places.
query
SELECT CASE WHEN id % 2 = 0
  THEN named_struct('x', CAST(id AS DOUBLE), 'X', CAST(NULL AS DOUBLE))
  ELSE named_struct('X', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE))
END FROM range(8)

-- An untyped NULL branch must not restore name-based struct coercion.
query
SELECT CASE WHEN id % 3 = 0
  THEN named_struct('x', CAST(id AS DOUBLE), 'X', CAST(NULL AS DOUBLE))
  WHEN id % 3 = 1 THEN NULL
  ELSE named_struct('X', CAST(NULL AS DOUBLE), 'x', CAST(id AS DOUBLE))
END FROM range(8)

-- IF must report and return the same nested type for uniform and mixed predicates.
query
SELECT IF(id < 0, CAST(NULL AS STRUCT<x:BIGINT>), named_struct('x', id)),
       IF(id >= 0, named_struct('x', id), CAST(NULL AS STRUCT<x:BIGINT>)),
       IF(id = 1, CAST(NULL AS STRUCT<x:BIGINT>), named_struct('x', id))
FROM range(8)

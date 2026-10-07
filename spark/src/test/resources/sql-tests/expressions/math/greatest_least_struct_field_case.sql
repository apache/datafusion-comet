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

-- With case-insensitive analysis, Spark accepts greatest and least arguments whose struct field
-- names differ only in case, and compares the structs field by field by position. The result
-- takes the first argument's field names. Here the second argument holds the same names in the
-- other order, so matching the fields by name instead would pair x with x and give a different
-- row for id = 0 and id = 2.

-- Config: spark.comet.exec.range.enabled=true
-- Config: spark.comet.sparkToColumnar.enabled=true
-- Config: spark.comet.sparkToColumnar.supportedOperatorList=Range
-- Config: spark.sql.caseSensitive=false

-- Float fields, which take Comet's own greatest and least
query
SELECT id,
  greatest(named_struct('x', CAST(id AS DOUBLE), 'X', 1D),
    named_struct('X', 1D, 'x', CAST(id AS DOUBLE)))
FROM range(4)

query
SELECT id,
  least(named_struct('x', CAST(id AS DOUBLE), 'X', 1D),
    named_struct('X', 1D, 'x', CAST(id AS DOUBLE)))
FROM range(4)

-- Integer fields, which take DataFusion's greatest and least
query
SELECT id,
  greatest(named_struct('x', CAST(id AS INT), 'X', 1),
    named_struct('X', 1, 'x', CAST(id AS INT))),
  least(named_struct('x', CAST(id AS INT), 'X', 1),
    named_struct('X', 1, 'x', CAST(id AS INT)))
FROM range(4)

-- More than two arguments
query
SELECT id,
  greatest(named_struct('x', CAST(id AS INT), 'X', 2),
    named_struct('X', 1, 'x', CAST(id AS INT)),
    named_struct('X', CAST(id AS INT), 'x', 1))
FROM range(4)

-- Structs nested in arrays
query
SELECT id,
  greatest(array(named_struct('x', CAST(id AS DOUBLE), 'X', 1D)),
    array(named_struct('X', 1D, 'x', CAST(id AS DOUBLE))))
FROM range(4)

query
SELECT id,
  least(array(named_struct('x', CAST(id AS INT), 'X', 1)),
    array(named_struct('X', 1, 'x', CAST(id AS INT))))
FROM range(4)

-- Arguments that also differ in whether a nested field can be NULL
query
SELECT id,
  greatest(named_struct('x', CAST(id AS DOUBLE), 'X', 1D),
    named_struct('X', IF(id = 3, NULL, 1D), 'x', CAST(id AS DOUBLE)))
FROM range(4)

query
SELECT id,
  least(array(named_struct('x', CAST(id AS INT), 'X', 1)),
    array(named_struct('X', 1, 'x', IF(id = 3, NULL, CAST(id AS INT)))))
FROM range(4)

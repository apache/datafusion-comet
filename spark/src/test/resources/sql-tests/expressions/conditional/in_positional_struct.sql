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

-- Unlike binary comparisons, IN accepts differently named structs on Spark 3.x too.
-- Dynamic integer and floating-point operands exercise both native membership paths.
query expect_native(in)
SELECT named_struct('x', id) IN (named_struct('y', id)),
       named_struct('x', CAST(id AS DOUBLE)) IN (named_struct('y', CAST(id AS DOUBLE)))
FROM range(3)

-- Align every candidate to one positional type, including a match after an initial miss.
query expect_native(in)
SELECT named_struct('x', id) IN (named_struct('y', id + 1), named_struct('z', id)),
       named_struct('x', CAST(id AS DOUBLE)) IN
         (named_struct('y', CAST(id + 1 AS DOUBLE)), named_struct('z', CAST(id AS DOUBLE)))
FROM range(3)

-- id=1 matches, while id=0 and id=2 do not. Matching names must not reorder field values.
query expect_native(in)
SELECT id, named_struct('x', id, 'y', 1L) IN (named_struct('y', 1L, 'x', id)),
           named_struct('x', id, 'y', 1L) NOT IN (named_struct('y', 1L, 'x', id))
FROM range(3)

-- Reconcile field names inside nested structs and list elements as well.
query expect_native(in)
SELECT named_struct('a', named_struct('x', id)) IN
         (named_struct('b', named_struct('y', id))),
       array(named_struct('x', CAST(id AS DOUBLE))) IN
         (array(named_struct('y', CAST(id AS DOUBLE))))
FROM range(3)

-- NULL candidates preserve unknown for a miss, but must not hide a later match.
query expect_native(in)
SELECT named_struct('x', id) IN
         (named_struct('y', id + 1), CAST(NULL AS STRUCT<z:BIGINT>)),
       named_struct('x', id) NOT IN
         (named_struct('y', id + 1), CAST(NULL AS STRUCT<z:BIGINT>)),
       named_struct('x', id) IN
         (CAST(NULL AS STRUCT<z:BIGINT>), named_struct('y', id))
FROM range(3)

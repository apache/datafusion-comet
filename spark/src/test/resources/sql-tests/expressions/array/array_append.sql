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

-- On Spark 4.0, array_append is a RuntimeReplaceable that rewrites to array_insert(-1),
-- so we need to allow the incompatible array_insert to run natively there.
-- Config: spark.comet.expression.ArrayInsert.allowIncompatible=true

statement
CREATE TABLE test_array_append(arr array<int>, val int) USING parquet

statement
INSERT INTO test_array_append VALUES (array(1, 2, 3), 4), (array(), 1), (NULL, 1), (array(1, 2), NULL)

query
SELECT array_append(arr, val) FROM test_array_append

-- column + literal
query
SELECT array_append(arr, 99) FROM test_array_append

-- literal + column
query
SELECT array_append(array(1, 2, 3), val) FROM test_array_append

-- literal + literal
query
SELECT array_append(array(1, 2, 3), 4), array_append(array(), 1), array_append(cast(NULL as array<int>), 1)

-- Arrays of maps. `CometCreateArray` widens every child to a deeply-nullable element type while
-- the appended item keeps Spark's own, so the kernel's declared result type and its two operand
-- types all have to agree with what the batch carries.
query
SELECT array_append(array(map(1, 2), map(3, 4)), map(5, 6)),
       array_append(array(map(1, array(2))), map(3, array(4)))

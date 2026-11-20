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

-- ConstantFolding: enabled

statement
CREATE TABLE folded_array_map_literals(id INT, value BIGINT) USING parquet

statement
INSERT INTO folded_array_map_literals SELECT CAST(id AS INT), id FROM range(0, 3, 1, 1)

-- sequence folds to an UnsafeArrayData literal. Project it over a table so Comet's
-- literal serializer reads it instead of evaluating the sequence expression.
query
SELECT sequence(1, 5) AS arr FROM folded_array_map_literals

-- The nested sequences also fold to UnsafeArrayData, exercising recursive list serialization.
query
SELECT array(sequence(1, 3), sequence(4, 5)) AS arr FROM folded_array_map_literals

-- The whole array folds to one ArrayType(MapType(...), containsNull=true) literal.
-- The NULL element and the rebuilt populated map must retain identical map types.
-- Three rows in one file exercise broadcasting the folded literal across a batch.
query
SELECT array(map(1, 2), NULL) AS arr FROM folded_array_map_literals

query
SELECT array(NULL, map(1, 2)) AS arr FROM folded_array_map_literals

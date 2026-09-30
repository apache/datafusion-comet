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

-- `element_at` on an in-bounds literal index is non-nullable, yet with ANSI off the code Spark
-- generates for it assigns an undeclared null flag, so it does not compile. Spark never falls
-- back while testing, so these queries run without generated code (NO_CODEGEN, whole-stage off)
-- for Spark to have an answer; the dispatcher then evaluates its kernel through `eval` from the
-- start, as Spark does. The compile-failure retry itself is covered in CometCodegenSuite.

-- Config: spark.sql.codegen.wholeStage=false
-- Config: spark.sql.codegen.factoryMode=NO_CODEGEN

statement
CREATE TABLE test_create_map_interpreted(id bigint) USING parquet

-- One file, so the rows share a batch.
statement
INSERT INTO test_create_map_interpreted SELECT id FROM range(0, 4, 1, 1)

query expect_dispatch(map)
SELECT id, map(0, named_struct('i', id), 1, element_at(array(named_struct('i', id)), 1)) FROM test_create_map_interpreted

query expect_dispatch(map)
SELECT id, map(0, named_struct('i', id, 'n', NULL), 1, element_at(array(named_struct('i', id, 'n', NULL)), 1)) FROM test_create_map_interpreted

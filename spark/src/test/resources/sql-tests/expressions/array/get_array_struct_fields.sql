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

-- ExcludeRules: org.apache.spark.sql.catalyst.optimizer.SimplifyExtractValueOps

statement
CREATE TABLE test_arr_struct(arr array<struct<name: string, value: int>>) USING parquet

statement
INSERT INTO test_arr_struct VALUES (array(named_struct('name', 'a', 'value', 1), named_struct('name', 'b', 'value', 2))), (array(named_struct('name', 'x', 'value', 10))), (NULL), (array(NULL, named_struct('name', 'b', 'value', 2)))

query spark_answer_only
SELECT arr.name, arr.value FROM test_arr_struct

-- A NullType field of an array of structs reads as an array of NULLs, also for null structs,
-- whose nulls are pushed into the all-null field. SimplifyExtractValueOps is excluded (see the
-- top of the file) so the field access is not rewritten into the array constructor.
statement
CREATE TABLE test_arr_struct_null_field(a int) USING parquet

statement
INSERT INTO test_arr_struct_null_field VALUES (1), (-1), (NULL)

query
SELECT arr.x, arr.n FROM (
  SELECT array(IF(a > 0, named_struct('x', a, 'n', null), null), named_struct('x', 0, 'n', null)) AS arr
  FROM test_arr_struct_null_field)

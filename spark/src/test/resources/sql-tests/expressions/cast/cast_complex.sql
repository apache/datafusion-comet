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

statement
CREATE TABLE test_cast_complex(
  id int,
  struct_arr struct<items:array<int>,label:string>,
  deep struct<outer:struct<middle:struct<value:string,flag:boolean>,numbers:array<string>>,note:string>,
  arr_struct array<struct<id:int,score:string>>
) USING parquet

statement
INSERT INTO test_cast_complex VALUES
  (
    1,
    named_struct(
      'items', array(1, 2, cast(null as int)),
      'label', 'first'),
    named_struct(
      'outer',
      named_struct(
        'middle', named_struct('value', '1', 'flag', true),
        'numbers', array('2', '3')),
      'note', 'alpha'),
    array(
      named_struct('id', 1, 'score', '10'),
      named_struct('id', 2, 'score', cast(null as string)))
  ),
  (
    2,
    named_struct(
      'items', cast(array() as array<int>),
      'label', cast(null as string)),
    named_struct(
      'outer',
      named_struct(
        'middle', named_struct('value', cast(null as string), 'flag', false),
        'numbers', array(cast(null as string))),
      'note', cast(null as string)),
    array(named_struct('id', cast(null as int), 'score', '30'))
  ),
  (
    3,
    cast(null as struct<items:array<int>,label:string>),
    cast(null as
      struct<outer:struct<middle:struct<value:string,flag:boolean>,numbers:array<string>>,
      note:string>),
    cast(array() as array<struct<id:int,score:string>>)
  ),
  (
    4,
    named_struct(
      'items', array(-1, 0, 2147483647),
      'label', 'edge'),
    named_struct(
      'outer',
      named_struct(
        'middle', named_struct('value', '-4', 'flag', true),
        'numbers', array('-5', '0')),
      'note', 'omega'),
    cast(null as array<struct<id:int,score:string>>)
  )

-- struct field containing an array
query
SELECT cast(struct_arr as struct<items:array<string>,label:string>), id
FROM test_cast_complex
ORDER BY id

-- struct fields can be renamed by the target type
query
SELECT cast(struct_arr as struct<numbers:array<string>,name:string>), id
FROM test_cast_complex
ORDER BY id

-- struct target field names are applied positionally
query
SELECT cast(struct_arr as struct<label:array<string>,items:string>), id
FROM test_cast_complex
ORDER BY id

-- missing struct fields are rejected
query expect_error(DATATYPE_MISMATCH)
SELECT cast(struct_arr as struct<items:array<string>>)
FROM test_cast_complex

-- extra struct fields are rejected
query expect_error(DATATYPE_MISMATCH)
SELECT cast(struct_arr as struct<items:array<string>,label:string,extra:int>)
FROM test_cast_complex

-- deeply nested struct to struct
query
SELECT cast(deep as
  struct<outer:struct<middle:struct<value:int,flag:string>,numbers:array<int>>,note:string>), id
FROM test_cast_complex
ORDER BY id

-- deeply nested struct to string
query
SELECT cast(deep as string), id
FROM test_cast_complex
ORDER BY id

-- array of structs to array of structs
query
SELECT cast(arr_struct as array<struct<id:bigint,score:int>>), id
FROM test_cast_complex
ORDER BY id

-- array of structs with renamed fields
query
SELECT cast(arr_struct as array<struct<item_id:bigint,total:int>>), id
FROM test_cast_complex
ORDER BY id

-- array of structs with target field names applied positionally
query
SELECT cast(arr_struct as array<struct<score:bigint,id:int>>), id
FROM test_cast_complex
ORDER BY id

-- array of structs with missing nested fields is rejected
query expect_error(DATATYPE_MISMATCH)
SELECT cast(arr_struct as array<struct<id:bigint>>)
FROM test_cast_complex

-- array of structs with extra nested fields is rejected
query expect_error(DATATYPE_MISMATCH)
SELECT cast(arr_struct as array<struct<id:bigint,score:int,extra:string>>)
FROM test_cast_complex

-- array of structs to string
query
SELECT cast(arr_struct as string), id
FROM test_cast_complex
ORDER BY id

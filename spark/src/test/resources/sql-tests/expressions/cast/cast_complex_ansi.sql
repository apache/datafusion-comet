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

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_cast_complex_ansi(
  id int,
  struct_value struct<
    long_value:bigint,
    string_value:string,
    nested_value:struct<inner_long:bigint>>,
  array_value array<bigint>
) USING parquet

statement
INSERT INTO test_cast_complex_ansi VALUES
  (
    1,
    named_struct(
      'long_value', cast(1 as bigint),
      'string_value', 'fits',
      'nested_value', named_struct('inner_long', cast(10 as bigint))),
    array(cast(1 as bigint), cast(127 as bigint), cast(null as bigint))
  ),
  (
    2,
    named_struct(
      'long_value', cast(128 as bigint),
      'string_value', 'too-large',
      'nested_value', named_struct('inner_long', cast(10 as bigint))),
    array(cast(1 as bigint))
  ),
  (
    3,
    named_struct(
      'long_value', cast(2 as bigint),
      'string_value', 'nested-too-small',
      'nested_value', named_struct('inner_long', cast(-129 as bigint))),
    array(cast(2 as bigint))
  ),
  (
    4,
    named_struct(
      'long_value', cast(3 as bigint),
      'string_value', 'array-too-large',
      'nested_value', named_struct('inner_long', cast(4 as bigint))),
    array(cast(128 as bigint))
  ),
  (
    5,
    cast(null as struct<
      long_value:bigint,
      string_value:string,
      nested_value:struct<inner_long:bigint>>),
    cast(null as array<bigint>)
  )

-- valid complex casts should run natively under ANSI mode
query
SELECT
  cast(struct_value as
    struct<byte_value:tinyint,text:string,nested_value:struct<inner_byte:tinyint>>),
  cast(array_value as array<tinyint>),
  id
FROM test_cast_complex_ansi
WHERE id IN (1, 5)
ORDER BY id

-- overflow in a struct field should propagate as a cast error
query expect_error(CAST_OVERFLOW)
SELECT cast(struct_value as
  struct<byte_value:tinyint,text:string,nested_value:struct<inner_byte:tinyint>>)
FROM test_cast_complex_ansi
WHERE id = 2

-- overflow in a nested struct field should propagate as a cast error
query expect_error(CAST_OVERFLOW)
SELECT cast(struct_value as
  struct<byte_value:tinyint,text:string,nested_value:struct<inner_byte:tinyint>>)
FROM test_cast_complex_ansi
WHERE id = 3

-- overflow in an array element should propagate as a cast error
query expect_error(CAST_OVERFLOW)
SELECT cast(array_value as array<tinyint>)
FROM test_cast_complex_ansi
WHERE id = 4

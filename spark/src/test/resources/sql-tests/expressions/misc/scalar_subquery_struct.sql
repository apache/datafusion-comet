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

-- Config: spark.sql.session.timeZone=UTC

statement
CREATE TABLE test_struct_subq(
  id int,
  payload struct<flag:boolean,tiny:tinyint,small:smallint,number:int,large:bigint,
                 single:float,dbl:double,amount:decimal(38,10),compact:decimal(10,2),
                 text:string,bytes:binary,day:date,instant:timestamp,local_time:timestamp_ntz,
                 nested:struct<last:string,first:bigint>>) USING parquet

statement
INSERT INTO test_struct_subq VALUES
  (1, named_struct(
    'flag', true, 'tiny', -128, 'small', -32768, 'number', -2147483648,
    'large', -9223372036854775808, 'single', 1.25, 'dbl', -2.5,
    'amount', 1234567890123456789012345678.1234567890, 'compact', -12345.67,
    'text', '中文-é', 'bytes', X'00FF41', 'day', DATE '1969-12-31',
    'instant', TIMESTAMP '1969-12-31 23:59:59.123456',
    'local_time', TIMESTAMP_NTZ '2024-02-29 12:34:56.654321',
    'nested', named_struct('last', 'tail', 'first', 9876543210))),
  (2, named_struct(
    'flag', NULL, 'tiny', NULL, 'small', NULL, 'number', NULL, 'large', NULL,
    'single', NULL, 'dbl', NULL, 'amount', NULL, 'compact', NULL, 'text', NULL,
    'bytes', NULL, 'day', NULL, 'instant', NULL, 'local_time', NULL, 'nested', NULL)),
  (3, NULL),
  (4, named_struct(
    'flag', false, 'tiny', 127, 'small', 32767, 'number', 2147483647,
    'large', 9223372036854775807, 'single', 0.0, 'dbl', 3.5,
    'amount', -9999999999999999999999999999.9999999999, 'compact', 0.00,
    'text', '', 'bytes', X'', 'day', DATE '2000-02-29',
    'instant', TIMESTAMP '2024-02-29 12:34:56.654321',
    'local_time', TIMESTAMP_NTZ '1969-12-31 23:59:59.123456',
    'nested', named_struct('last', NULL, 'first', NULL)))

-- Materialize the entire non-null scalar struct for every outer row.
query
SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 1) AS s
FROM test_struct_subq

-- Distinct field values and deliberately nonalphabetical nested names catch ordinal mixups.
query
SELECT id, s.flag, s.tiny, s.small, s.number, s.large, s.single, s.dbl,
       s.amount, s.compact, s.text, s.bytes, s.day, s.instant, s.local_time,
       s.nested.last, s.nested.first
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 1) AS s
      FROM test_struct_subq)

-- A present struct with all-null fields must not become a null struct.
query
SELECT id, s, s IS NULL, s.number, s.nested IS NULL
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 2) AS s
      FROM test_struct_subq)

-- A null struct result stays null when materialized and when its fields are extracted.
query
SELECT id, s, s IS NULL, s.number, s.nested, s.nested.first
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 3) AS s
      FROM test_struct_subq)

-- A present nested struct whose children are null has its own validity bit.
query
SELECT id, s, s.nested IS NULL, s.nested.last, s.nested.first
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 4) AS s
      FROM test_struct_subq)

-- A scalar subquery with no rows returns a null struct of the declared type.
query
SELECT id, s, s IS NULL, s.number, s.nested.first
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 99) AS s
      FROM test_struct_subq)

-- Separate scalar subqueries of the same type must retain separate results.
query
SELECT id,
       (SELECT payload FROM test_struct_subq WHERE id = 1),
       (SELECT payload FROM test_struct_subq WHERE id = 2),
       (SELECT payload FROM test_struct_subq WHERE id = 3),
       (SELECT payload FROM test_struct_subq WHERE id = 4)
FROM test_struct_subq

-- Untyped null fields cannot be stored in Parquet, so construct them in the subquery.
query
SELECT id, (SELECT named_struct('untyped', NULL, 'value', max(id),
                               'nested', named_struct('untyped', NULL, 'value', min(id)))
            FROM test_struct_subq) AS s
FROM test_struct_subq

statement
CREATE TABLE test_struct_subq_unsupported(id int, items array<int>, entries map<string,int>)
USING parquet

statement
INSERT INTO test_struct_subq_unsupported VALUES
  (1, array(10, NULL, 30), map('a', 10, 'b', NULL)),
  (2, NULL, NULL)

-- Supporting structs must not enable array or map scalar results.
query expect_fallback(Unsupported data type)
SELECT id, (SELECT items FROM test_struct_subq_unsupported WHERE id = 1)
FROM test_struct_subq_unsupported

query expect_fallback(Unsupported data type)
SELECT id, (SELECT entries FROM test_struct_subq_unsupported WHERE id = 1)
FROM test_struct_subq_unsupported

-- Unsupported fields must also be rejected recursively inside structs.
query expect_fallback(Unsupported data type)
SELECT id, (SELECT named_struct('nested', named_struct('items', items))
            FROM test_struct_subq_unsupported WHERE id = 1)
FROM test_struct_subq_unsupported

query expect_fallback(Unsupported data type)
SELECT id, (SELECT named_struct('nested', named_struct('entries', entries))
            FROM test_struct_subq_unsupported WHERE id = 1)
FROM test_struct_subq_unsupported

-- Duplicate field names are unsupported at the top level and in nested structs.
query expect_fallback(Unsupported data type)
SELECT id, (SELECT named_struct('same', id, 'same', id + 1)
            FROM test_struct_subq_unsupported WHERE id = 1)
FROM test_struct_subq_unsupported

query expect_fallback(Unsupported data type)
SELECT id, (SELECT named_struct('nested', named_struct('same', id, 'same', id + 1))
            FROM test_struct_subq_unsupported WHERE id = 1)
FROM test_struct_subq_unsupported

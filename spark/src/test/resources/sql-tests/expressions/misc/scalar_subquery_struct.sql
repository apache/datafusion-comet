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

-- ConfigMatrix: spark.sql.session.timeZone=UTC,America/Los_Angeles
-- Enable subquery merging and exercise padding fallback with the dispatcher enabled.
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.execution.reuseSubquery=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

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

-- Cast from strings: a decimal -0.0 literal would lose the floating-point sign.
statement
INSERT INTO test_struct_subq
SELECT id, named_struct(
  'flag', NULL, 'tiny', NULL, 'small', NULL, 'number', NULL, 'large', NULL,
  'single', CAST(value AS FLOAT), 'dbl', CAST(value AS DOUBLE),
  'amount', NULL, 'compact', NULL, 'text', NULL, 'bytes', NULL, 'day', NULL,
  'instant', NULL, 'local_time', NULL, 'nested', NULL)
FROM VALUES (5, '-0.0'), (6, 'NaN'), (7, 'Infinity'), (8, '-Infinity') AS special(id, value)

-- Equality checks zero semantics; string casts also detect a lost negative-zero sign.
query
SELECT id, s.single = 0.0, s.dbl = 0.0,
       CAST(s.single AS STRING), CAST(s.dbl AS STRING)
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 5) AS s
      FROM test_struct_subq)

query
SELECT id, isnan(s.single), isnan(s.dbl),
       CAST(s.single AS STRING), CAST(s.dbl AS STRING)
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 6) AS s
      FROM test_struct_subq)

query
SELECT id, s.single > 0, s.dbl > 0,
       CAST(s.single AS STRING), CAST(s.dbl AS STRING)
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 7) AS s
      FROM test_struct_subq)

query
SELECT id, s.single < 0, s.dbl < 0,
       CAST(s.single AS STRING), CAST(s.dbl AS STRING)
FROM (SELECT id, (SELECT payload FROM test_struct_subq WHERE id = 8) AS s
      FROM test_struct_subq)

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

-- Consume the timestamp under each session zone, rather than only displaying it.
query
SELECT id, CAST(s.instant AS STRING),
       s.instant = TIMESTAMP '1969-12-31 23:59:59.123456'
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

-- hour: plain scalar and paired aggregate subqueries, including runtime NULL results.
query expect_native(hour)
SELECT id, hour((SELECT max(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(hour)
SELECT id, hour((SELECT max(payload.instant) FROM test_struct_subq)),
       hour((SELECT min(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(hour)
SELECT id, hour((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

query expect_native(hour)
SELECT id, hour((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0)),
       hour((SELECT min(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

-- minute: plain scalar and paired aggregate subqueries, including runtime NULL results.
query expect_native(minute)
SELECT id, minute((SELECT max(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(minute)
SELECT id, minute((SELECT max(payload.instant) FROM test_struct_subq)),
       minute((SELECT min(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(minute)
SELECT id, minute((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

query expect_native(minute)
SELECT id, minute((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0)),
       minute((SELECT min(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

-- second: plain scalar and paired aggregate subqueries, including runtime NULL results.
query expect_native(second)
SELECT id, second((SELECT max(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(second)
SELECT id, second((SELECT max(payload.instant) FROM test_struct_subq)),
       second((SELECT min(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(second)
SELECT id, second((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

query expect_native(second)
SELECT id, second((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0)),
       second((SELECT min(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

-- unix_timestamp: plain scalar and paired aggregate subqueries, including runtime NULL results.
query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(payload.instant) FROM test_struct_subq)),
       unix_timestamp((SELECT min(payload.instant) FROM test_struct_subq))
FROM test_struct_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

query expect_native(unix_timestamp)
SELECT id, unix_timestamp((SELECT max(payload.instant) FROM test_struct_subq WHERE id < 0)),
       unix_timestamp((SELECT min(payload.instant) FROM test_struct_subq WHERE id < 0))
FROM test_struct_subq

-- lpad: scalar strings fall back for both plain and paired aggregate subqueries.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, lpad((SELECT max(payload.text) FROM test_struct_subq), 8, '0')
FROM test_struct_subq

query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, lpad((SELECT max(payload.text) FROM test_struct_subq), 8, '0'),
       lpad((SELECT min(payload.text) FROM test_struct_subq), 8, '0')
FROM test_struct_subq

query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, lpad((SELECT max(payload.text) FROM test_struct_subq WHERE id < 0), 1000000, '0')
FROM test_struct_subq

query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, lpad((SELECT max(payload.text) FROM test_struct_subq WHERE id < 0), 1000000, '0'),
       lpad((SELECT min(payload.text) FROM test_struct_subq WHERE id < 0), 1000000, '0')
FROM test_struct_subq

-- A scalar string with per-row lengths also stays on Spark's fallback path.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, lpad((SELECT max(payload.text) FROM test_struct_subq), id, '0')
FROM test_struct_subq

-- Wrapping the scalar string must not bypass the fallback guard.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, lpad(concat((SELECT max(payload.text) FROM test_struct_subq), ''), 8, '0')
FROM test_struct_subq

-- Column strings still run natively with plain or paired scalar subquery lengths.
query expect_native(lpad)
SELECT id, lpad(payload.text, (SELECT max(id) FROM test_struct_subq), '0')
FROM test_struct_subq

query expect_native(lpad)
SELECT id, lpad(payload.text, (SELECT max(id) FROM test_struct_subq), '0'),
       lpad(payload.text, (SELECT min(id) FROM test_struct_subq), '0')
FROM test_struct_subq

-- Runtime NULL lengths must reach the column kernel without becoming literal NULLs.
query expect_native(lpad)
SELECT id, lpad(payload.text, (SELECT max(id) FROM test_struct_subq WHERE id < 0), '0')
FROM test_struct_subq

query expect_native(lpad)
SELECT id, lpad(payload.text, (SELECT max(id) FROM test_struct_subq WHERE id < 0), '0'),
       lpad(payload.text, (SELECT min(id) FROM test_struct_subq WHERE id < 0), '0')
FROM test_struct_subq

-- rpad: scalar strings fall back for both plain and paired aggregate subqueries.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, rpad((SELECT max(payload.text) FROM test_struct_subq), 8, '0')
FROM test_struct_subq

query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, rpad((SELECT max(payload.text) FROM test_struct_subq), 8, '0'),
       rpad((SELECT min(payload.text) FROM test_struct_subq), 8, '0')
FROM test_struct_subq

query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, rpad((SELECT max(payload.text) FROM test_struct_subq WHERE id < 0), 1000000, '0')
FROM test_struct_subq

query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, rpad((SELECT max(payload.text) FROM test_struct_subq WHERE id < 0), 1000000, '0'),
       rpad((SELECT min(payload.text) FROM test_struct_subq WHERE id < 0), 1000000, '0')
FROM test_struct_subq

-- A scalar string with per-row lengths also stays on Spark's fallback path.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, rpad((SELECT max(payload.text) FROM test_struct_subq), id, '0')
FROM test_struct_subq

-- Wrapping the scalar string must not bypass the fallback guard.
query expect_fallback(Scalar values are not supported for the `str` argument)
SELECT id, rpad(concat((SELECT max(payload.text) FROM test_struct_subq), ''), 8, '0')
FROM test_struct_subq

-- Column strings still run natively with plain or paired scalar subquery lengths.
query expect_native(rpad)
SELECT id, rpad(payload.text, (SELECT max(id) FROM test_struct_subq), '0')
FROM test_struct_subq

query expect_native(rpad)
SELECT id, rpad(payload.text, (SELECT max(id) FROM test_struct_subq), '0'),
       rpad(payload.text, (SELECT min(id) FROM test_struct_subq), '0')
FROM test_struct_subq

-- Runtime NULL lengths must reach the column kernel without becoming literal NULLs.
query expect_native(rpad)
SELECT id, rpad(payload.text, (SELECT max(id) FROM test_struct_subq WHERE id < 0), '0')
FROM test_struct_subq

query expect_native(rpad)
SELECT id, rpad(payload.text, (SELECT max(id) FROM test_struct_subq WHERE id < 0), '0'),
       rpad(payload.text, (SELECT min(id) FROM test_struct_subq WHERE id < 0), '0')
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

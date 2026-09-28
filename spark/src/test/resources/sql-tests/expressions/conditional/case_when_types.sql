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

-- CASE WHEN, IF and COALESCE for each result type. When every branch is a column, a literal, or a
-- comparison or wrapping arithmetic over them, Comet evaluates the branches for the whole batch
-- and then picks each row's value, and it does that differently for each type. NULLs appear both
-- in the predicates, where they must not match, and in the values.

statement
CREATE TABLE test_case_types(
  k int, p boolean, q boolean,
  b boolean, t tinyint, sm smallint, i int, l bigint, f float, d double,
  dec decimal(10, 2), wdec decimal(38, 18), dt date, ts timestamp, ntz timestamp_ntz,
  s string, bin binary) USING parquet

statement
INSERT INTO test_case_types VALUES
  (0, true, false, true, 1, 10, 100, 1000, 1.5, 2.5, 12.34, 1.000000000000000001,
   DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP_NTZ '2024-01-01 10:00:00',
   'apple', X'01'),
  (1, false, true, false, -2, -20, -200, -2000, -2.5, -3.5, -45.67, -2.5,
   DATE '1999-12-31', TIMESTAMP '1999-12-31 23:59:59', TIMESTAMP_NTZ '1999-12-31 23:59:59',
   'banana', X'0203'),
  (2, NULL, true, true, 3, 30, 300, 3000, 3.25, 4.25, 0.01, 0.000000000000000001,
   DATE '2000-02-29', TIMESTAMP '2000-02-29 12:30:00', TIMESTAMP_NTZ '2000-02-29 12:30:00',
   '', X''),
  (3, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
   NULL, NULL, NULL, NULL, NULL),
  (4, false, false, false, 127, 32767, 2147483647, 9223372036854775807, 3.4028235E38,
   1.7976931348623157E308, 99999999.99, 99999999999999999999.999999999999999999,
   DATE '9999-12-31', TIMESTAMP '2262-04-11 23:47:16', TIMESTAMP_NTZ '2262-04-11 23:47:16',
   'naïve café', X'FFFE'),
  (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
   NULL, NULL, NULL, NULL, NULL),
  (6, true, true, true, -128, -32768, -2147483648, -9223372036854775808,
   CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE), -99999999.99,
   -99999999999999999999.999999999999999999,
   DATE '1901-01-01', TIMESTAMP '1901-01-01 00:00:01', TIMESTAMP_NTZ '1901-01-01 00:00:01',
   'a value that is longer than sixteen bytes', X'00112233445566778899AABBCCDDEEFF0011'),
  (7, false, NULL, false, 0, 0, 0, 0, CAST('-Infinity' AS FLOAT), CAST('Infinity' AS DOUBLE),
   0.00, 0.0, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00',
   TIMESTAMP_NTZ '1970-01-01 00:00:00', '日本語', X'00'),
  (8, true, false, NULL, 5, 50, 500, 5000, 5.5, 6.5, 5.55, 5.5, DATE '2024-06-30',
   TIMESTAMP '2024-06-30 06:00:00', TIMESTAMP_NTZ '2024-06-30 06:00:00', NULL, NULL),
  (9, false, true, true, NULL, 60, NULL, 6000, NULL, 7.5, NULL, 6.5, NULL,
   TIMESTAMP '2024-07-01 07:00:00', NULL, 'z', X'7A')

query
SELECT k,
  CASE WHEN p THEN b END,
  CASE WHEN p THEN b ELSE q END,
  CASE WHEN p THEN b WHEN q THEN false ELSE NULL END,
  IF(q, b, true),
  coalesce(b, p, q)
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN t END,
  CASE WHEN p THEN t ELSE sm END,
  IF(q, sm, CAST(-1 AS smallint)),
  coalesce(t, sm)
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN i END,
  CASE WHEN p THEN i ELSE -1 END,
  CASE WHEN p THEN i WHEN q THEN 42 ELSE NULL END,
  CASE WHEN p THEN i ELSE l END,
  IF(q, NULL, i),
  coalesce(i, 7)
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN l END,
  CASE WHEN p THEN l WHEN q THEN l + 1 ELSE l * 2 END,
  IF(q, l, l - i),
  coalesce(l, i, 0)
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN f END,
  CASE WHEN p THEN f ELSE d END,
  CASE WHEN p THEN d WHEN q THEN d / 2 ELSE CAST('NaN' AS DOUBLE) END,
  IF(q, d, d * d),
  coalesce(f, d)
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN dec END,
  CASE WHEN p THEN dec ELSE 0 END,
  IF(q, dec, NULL),
  CASE WHEN p THEN wdec WHEN q THEN wdec ELSE NULL END,
  coalesce(dec, 1.5)
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN dt END,
  CASE WHEN p THEN dt ELSE DATE '1970-01-02' END,
  IF(q, dt, NULL),
  coalesce(dt, DATE '2000-01-01')
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN ts END,
  CASE WHEN p THEN ts WHEN q THEN TIMESTAMP '2020-01-01 00:00:00' END,
  IF(q, ntz, NULL),
  CASE WHEN p THEN ntz ELSE TIMESTAMP_NTZ '2020-01-01 00:00:00' END,
  coalesce(ts, TIMESTAMP '2000-01-01 00:00:00')
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN s END,
  CASE WHEN p THEN s ELSE 'else' END,
  CASE WHEN p THEN s WHEN q THEN 'q' ELSE NULL END,
  CASE WHEN p THEN 'p' WHEN q THEN 'a longer literal than sixteen bytes' ELSE '' END,
  IF(p, 'a', IF(q, 'b', s)),
  coalesce(s, 'none')
FROM test_case_types

query
SELECT k,
  CASE WHEN p THEN bin END,
  CASE WHEN p THEN bin ELSE X'FF' END,
  IF(q, bin, NULL),
  coalesce(bin, X'')
FROM test_case_types

-- nested results are merged by DataFusion's CASE
query
SELECT k,
  CASE WHEN p THEN array(i, 1) ELSE array(2) END,
  IF(q, map('k', i), map('k', i + 1)),
  CASE WHEN p THEN named_struct('a', i, 'b', s) ELSE named_struct('a', i + 1, 'b', s) END,
  CASE WHEN p THEN map('k', i) ELSE map('k', 0) END
FROM test_case_types

-- IF fails when its branches differ only in nested nullability
query ignore(https://github.com/apache/datafusion-comet/issues/6334)
SELECT k, IF(q, map('k', i), map('k', 0)) FROM test_case_types

-- literal predicates, which constant folding would otherwise remove
query
SELECT CASE WHEN true THEN 1 ELSE 2 END, CASE WHEN false THEN 1 ELSE 2 END,
  CASE WHEN CAST(NULL AS BOOLEAN) THEN 1 ELSE 2 END, IF(true, 'a', 'b'), IF(NULL, 'a', 'b')

-- enough rows for several batches and many words of each predicate's bitmap, both in a random
-- order and in the ascending order that gives each branch one run of rows
statement
CREATE TABLE test_case_many(id bigint, v bigint, s string) USING parquet

statement
INSERT INTO test_case_many
SELECT id,
  IF(id % 13 = 0, NULL, (id * 7919) % 20011 - 10000),
  IF(id % 17 = 0, NULL, concat('v', CAST(id * 31 % 1000 AS STRING)))
FROM range(0, 20000)

query
SELECT id,
  CASE WHEN v < -5000 THEN 'low' WHEN v < 0 THEN s WHEN v < 5000 THEN 'mid' END,
  CASE WHEN v < -5000 THEN v WHEN v < 0 THEN v + id WHEN v < 5000 THEN id ELSE -1 END,
  IF(v < 0, s, concat(s, '!')),
  coalesce(v, id)
FROM test_case_many

query
SELECT id,
  CASE WHEN id < 5000 THEN v WHEN id < 12000 THEN v * 2 ELSE 0 END,
  CASE WHEN id < 7000 THEN s WHEN id < 7100 THEN 'narrow' ELSE NULL END,
  IF(id < 10000, id, v)
FROM test_case_many

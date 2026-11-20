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

-- MinSparkVersion: 4.0

-- Spark compares a collated string inside an array_distinct or array_union element under its
-- collation, so 'a' and 'A' are one value under UTF8_LCASE. The native kernels for elements with
-- a float normalize the floats but compare strings by their bytes, so elements that hold both
-- fall back to Spark on every version, including the releases whose floats run natively.
-- A cast keeps the collated strings in the Comet plan, unlike collate(), which falls back itself.

statement
CREATE TABLE array_set_collated(s1 string, s2 string, d double) USING parquet

statement
INSERT INTO array_set_collated VALUES
  ('a', 'A', 1.0),
  ('a', 'b', 1.0),
  ('x', 'X', 0.0),
  ('n', 'N', NULL),
  (NULL, 'A', 2.0)

query expect_fallback(non-UTF8_BINARY collated string)
SELECT
  size(array_distinct(array(
    named_struct('s', CAST(s1 AS STRING COLLATE UTF8_LCASE), 'd', d),
    named_struct('s', CAST(s2 AS STRING COLLATE UTF8_LCASE), 'd', d)))),
  array_distinct(array(
    named_struct('s', CAST(s1 AS STRING COLLATE UTF8_LCASE), 'd', d),
    named_struct('s', CAST(s2 AS STRING COLLATE UTF8_LCASE), 'd', d)))
FROM array_set_collated

query expect_fallback(non-UTF8_BINARY collated string)
SELECT
  size(array_union(
    array(named_struct('s', CAST(s1 AS STRING COLLATE UTF8_LCASE), 'd', d)),
    array(named_struct('s', CAST(s2 AS STRING COLLATE UTF8_LCASE), 'd', d)))),
  array_union(
    array(named_struct('s', CAST(s1 AS STRING COLLATE UTF8_LCASE), 'd', d)),
    array(named_struct('s', CAST(s2 AS STRING COLLATE UTF8_LCASE), 'd', d)))
FROM array_set_collated

-- The float differs only in the sign of zero, so a match needs both the collation and the
-- float normalization.
query expect_fallback(non-UTF8_BINARY collated string)
SELECT
  size(array_distinct(array(
    named_struct('s', CAST(s1 AS STRING COLLATE UTF8_LCASE), 'd', d),
    named_struct('s', CAST(s2 AS STRING COLLATE UTF8_LCASE), 'd', -d)))),
  size(array_union(
    array(named_struct('s', CAST(s1 AS STRING COLLATE UTF8_LCASE), 'd', d)),
    array(named_struct('s', CAST(s2 AS STRING COLLATE UTF8_LCASE), 'd', -d))))
FROM array_set_collated

-- Nested one level deeper: the collated string sits in an array inside the struct.
query expect_fallback(non-UTF8_BINARY collated string)
SELECT size(array_distinct(array(
    named_struct('s', array(CAST(s1 AS STRING COLLATE UTF8_LCASE)), 'd', d),
    named_struct('s', array(CAST(s2 AS STRING COLLATE UTF8_LCASE)), 'd', d))))
FROM array_set_collated

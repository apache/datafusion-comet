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

-- to_csv runs through the codegen dispatcher by default so results match Spark exactly, including
-- quoting and escaping. The native path is opt-in via
-- spark.comet.expression.StructsToCsv.allowIncompatible.

statement
CREATE TABLE test_to_csv(a int, b string, c double) USING parquet

statement
INSERT INTO test_to_csv VALUES
  (1, 'x', 2.5),
  (-3, 'hello,world', 0.0),
  (0, 'has "quote"', -1.5),
  (NULL, NULL, NULL),
  (7, '', 3.0)

-- column struct: values with delimiters and quotes exercise Spark's CSV quoting rules
query
SELECT to_csv(named_struct('a', a, 'b', b, 'c', c)) FROM test_to_csv

-- literal struct (constant folding is disabled by the test suite)
query
SELECT
  to_csv(named_struct('a', 1, 'b', 'x', 'c', 2.5)),
  to_csv(named_struct('s', 'a,b', 'n', CAST(NULL AS INT)))

-- Options are the main axis of behavior difference for this expression, and the dispatcher path
-- had no options coverage anywhere (CometCsvExpressionSuite only varies them under
-- allowIncompatible=true). `sep` also changes which values need quoting.
query
SELECT to_csv(named_struct('a', a, 'b', b, 'c', c), map('sep', ';')) FROM test_to_csv

-- timestampFormat over date and timestamp fields: these are the #3232 types that were
-- Incompatible before this change and now route through the dispatcher.
-- BinaryType is the other #3232 type but is deliberately absent: on Spark 3.4 / 3.5 the CSV
-- converter has no binary branch and renders it with Java's default Object.toString(), e.g.
-- "[B@10bc15e4", an identity hash that differs between any two evaluations, so the value is not
-- assertable by any engine including Spark itself. (Spark 4.0 added a real binary formatter, but
-- this fixture runs on every supported version.)
statement
CREATE TABLE test_to_csv_temporal(d date, t timestamp) USING parquet

statement
INSERT INTO test_to_csv_temporal VALUES
  (DATE '2024-01-31', TIMESTAMP '2024-01-31 12:34:56.789'),
  (DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00'),
  (NULL, NULL)

query
SELECT to_csv(named_struct('d', d, 't', t)) FROM test_to_csv_temporal

query
SELECT to_csv(named_struct('d', d, 't', t), map('timestampFormat', 'yyyy/MM/dd HH:mm', 'dateFormat', 'dd-MM-yyyy')) FROM test_to_csv_temporal

-- Complex field types (arrays, maps, nested structs). `StructsToCsv.checkInputDataTypes` accepts
-- them and `CometBatchKernelCodegen.isSupportedDataType` recurses into them, so they pass the
-- plan-time gate and reach the runtime dispatch. That is the accepted-at-plan-time /
-- rejected-at-runtime shape #5219 found for TIME types: `canHandle` greenlights the expression
-- before the plan commits, so a runtime gap is an execute-time failure with no fallback left.
--
-- Only non-nullness is asserted here, because the rendered value is not comparable on every
-- supported Spark version. Spark 3.4 / 3.5's `UnivocityGenerator.makeConverter` has no branch for
-- complex types and lands on `getter.get(ordinal, dataType).toString`, an identity string such as
-- "org.apache.spark.sql.vectorized.ColumnarArray@1ada50f0" whose hash differs between any two
-- evaluations and whose class differs between Spark's converter input (`ColumnarArray` /
-- `UnsafeArrayData`) and the kernel's (`InputArray_*`). That generic `get` is exactly what
-- `CometSpecializedGettersDispatch` implements for `CometInternalRow` / `CometArrayData`, so on
-- those versions these queries are the coverage for it. Spark 4.0 added real array/map/struct
-- converters, which render deterministically; `to_csv_nested.sql` asserts those values in full.
statement
CREATE TABLE test_to_csv_nested(s struct<i: int, arr: array<int>, m: map<string, int>, n: struct<x: int, y: string>>) USING parquet

statement
INSERT INTO test_to_csv_nested VALUES
  (named_struct('i', 1, 'arr', array(1, 2, 3), 'm', map('k', 10), 'n', named_struct('x', 5, 'y', 'z'))),
  (named_struct('i', NULL, 'arr', CAST(NULL AS array<int>), 'm', CAST(NULL AS map<string, int>), 'n', CAST(NULL AS struct<x: int, y: string>))),
  (CAST(NULL AS struct<i: int, arr: array<int>, m: map<string, int>, n: struct<x: int, y: string>>))

-- Struct column straight from the scan: the kernel reads it with getStruct, so the converter's
-- per-field reads of the array / map / struct fields all land on `CometInternalRow`. The all-null
-- and null-struct rows keep the assertion from collapsing to a constant true.
query
SELECT to_csv(s) IS NOT NULL FROM test_to_csv_nested

-- named_struct over the complex fields: the kernel's array / map / struct readers produce the
-- values that CreateNamedStruct stores into the row the converter then reads.
query
SELECT to_csv(named_struct('arr', s.arr, 'm', s.m, 'n', s.n)) IS NOT NULL FROM test_to_csv_nested

-- Complex types nested inside complex types, so the converter recurses through the kernel's
-- element getters rather than stopping at the first level.
query
SELECT to_csv(named_struct('aa', array(array(1, 2), array(3)), 'ms', map('k', named_struct('x', 1)))) IS NOT NULL FROM test_to_csv_nested

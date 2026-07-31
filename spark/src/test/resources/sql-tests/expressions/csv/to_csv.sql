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
-- BinaryType is the other #3232 type but is deliberately absent: Spark's CSV converter renders it
-- with Java's default Object.toString(), e.g. "[B@10bc15e4", an identity hash that differs between
-- any two evaluations, so the value is not assertable by any engine including Spark itself.
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

-- Complex field types (arrays, maps, nested structs) are deliberately not asserted here.
-- StructsToCsv.checkInputDataTypes accepts them, so they reach the converter, but Spark's own
-- output for them is not a value that can be compared: a non-null array/map/struct renders as a
-- Java identity string such as "org.apache.spark.sql.vectorized.ColumnarArray@1ada50f0", and any
-- null complex value throws NullPointerException inside Spark's UnsafeWriter.write. Both were
-- confirmed against Spark 3.5 with Comet disabled entirely, so they are Spark behavior, not
-- Comet's. Before this change these schemas were Unsupported and fell the projection back to
-- Spark; now they reach the dispatcher, which runs the same Spark code, so the observable result
-- is unchanged either way.

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

-- Dictionary-encoded dates reuse the scalar Date32 path for dictionary values.
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_trunc_date(d date) USING parquet

statement
INSERT INTO test_trunc_date VALUES
  (date('2024-05-17')),
  -- Monday / Tuesday / Sunday for Monday-based WEEK truncation.
  (date('2024-05-13')),
  (date('2024-05-14')),
  (date('2024-05-19')),
  (date('2024-01-01')),
  (date('2023-12-31')),
  (date('2024-03-31')),
  (date('2024-04-01')),
  (date('2024-06-30')),
  (date('2024-07-01')),
  (date('2024-10-01')),
  (date('2024-02-29')),
  (date('2000-02-29')),
  (date('1900-02-28')),
  (date('1969-12-31')),
  (date('1960-02-29')),
  (date('1900-01-01')),
  -- Valid Spark Date32 outside TimestampNanosecond's range.
  (date('3333-05-17')),
  (NULL)

query
SELECT d, trunc(d, 'YEAR'), trunc(d, 'YYYY'), trunc(d, 'YY') FROM test_trunc_date ORDER BY d

query
SELECT d, trunc(d, 'QUARTER') FROM test_trunc_date ORDER BY d

query
SELECT d, trunc(d, 'MONTH'), trunc(d, 'MON'), trunc(d, 'MM') FROM test_trunc_date ORDER BY d

query
SELECT d, trunc(d, 'WEEK') FROM test_trunc_date ORDER BY d

query
SELECT d, trunc(d, 'year'), trunc(d, 'Year'), trunc(d, 'yEaR'), trunc(d, 'month'), trunc(d, 'Mon'), trunc(d, 'week') FROM test_trunc_date ORDER BY d

-- Unsupported scalar formats fall back to Spark's codegen dispatcher and remain NULL-compatible.
query
SELECT d, trunc(d, 'DAY'), trunc(d, 'HOUR'), trunc(d, 'SECOND'), trunc(d, 'invalid'), trunc(d, ' YEAR '), trunc(d, '') FROM test_trunc_date ORDER BY d

-- NULL format is Incompatible on the native path, so this uses the codegen dispatcher.
query
SELECT d, trunc(d, NULL) FROM test_trunc_date ORDER BY d

query
SELECT trunc(NULL, 'YEAR'), trunc(NULL, NULL)

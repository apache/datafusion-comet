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

-- Config: spark.comet.shuffle.mode=native

statement
CREATE TABLE test_length(s string) USING parquet

statement
INSERT INTO test_length VALUES (''), ('a'), ('hello'), (NULL), ('café')

query
SELECT length(s), char_length(s) FROM test_length

-- literal arguments
query
SELECT length('hello'), length(''), length(NULL)

-- BinaryType input runs natively. length counts bytes for binary and characters for strings,
-- so the same text gives a different answer through each column.
statement
CREATE TABLE test_length_binary(s string, b binary, h string, r struct<b: binary>) USING parquet

statement
INSERT INTO test_length_binary VALUES
  ('hello', X'68656C6C6F', '68656C6C6F', named_struct('b', X'68656C6C6F')),
  (CAST(X'C3A9' AS STRING), X'C3A9', 'C3A9', named_struct('b', X'C3A9')),
  (CAST(X'F09F9880' AS STRING), X'F09F9880', 'F09F9880', named_struct('b', X'F09F9880')),
  ('', X'', '', named_struct('b', X'')),
  (NULL, NULL, NULL, named_struct('b', CAST(NULL AS BINARY)))

query
SELECT s, length(s), length(b) FROM test_length_binary

-- binary nested in a struct field
query
SELECT length(r.b) FROM test_length_binary

-- binary produced by unhex rather than read from the table
query
SELECT length(unhex(h)) FROM test_length_binary

-- substring on binary yields binary, so length counts the bytes of the slice
query
SELECT length(substring(b, 1, 2)), length(substring(b, 2)) FROM test_length_binary

-- Spark parses char_length and character_length to the same Length expression, so they accept binary
query
SELECT char_length(b), character_length(b) FROM test_length_binary

-- literal arguments
query
SELECT length(X'00FF'), length(X''), length(CAST(NULL AS BINARY)), length(unhex('C3A9'))

-- bytes that are not valid UTF-8 and embedded NUL bytes count as bytes, never as text
query
SELECT length(X'FF'), length(X'0000'), length(X'C3'), length(X'FFFE0000')

-- a string cast to binary counts its UTF-8 bytes, so the two lengths differ on multi-byte text
query
SELECT length(CAST(s AS BINARY)), length(s) FROM test_length_binary

-- binary inside an array element and a map value
query
SELECT length(array(b, X'01')[0]), length(map(1, b)[1]) FROM test_length_binary

-- the Int32 result takes part in arithmetic, a filter and a native aggregate
query
SELECT length(b) + 1, length(b) * 2 FROM test_length_binary WHERE length(b) >= 0

query
SELECT length(b) AS n, count(*) FROM test_length_binary GROUP BY length(b)

-- the binary column crosses a native shuffle before length reads it
query
SELECT length(b), length(r.b) FROM test_length_binary DISTRIBUTE BY b

-- a column that is NULL on every row
statement
CREATE TABLE test_length_all_null(b binary) USING parquet

statement
INSERT INTO test_length_all_null VALUES (CAST(NULL AS BINARY)), (CAST(NULL AS BINARY))

query
SELECT length(b), char_length(b) FROM test_length_all_null

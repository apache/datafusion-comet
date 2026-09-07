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

-- unbase64 runs on a native Rust kernel that ports the JDK's
-- java.util.Base64.getMimeDecoder() rules: bytes outside the base64 alphabet are skipped, and
-- the terminal-shape errors are reproduced with matching messages. This fixture pins the
-- happy-path shapes; the terminal-shape error messages are pinned in the Rust unit tests, since
-- Spark surfaces them as raw IllegalArgumentException without a SQL error class.

statement
CREATE TABLE test_unbase64(s string) USING parquet

-- 'YWJj' -> 'abc' (no padding), 'YQ==' -> 'a' (two-pad), 'YWI=' -> 'ab' (one-pad), '' -> '',
-- NULL -> NULL, 'YW Jj' -> 'abc' (embedded space, skipped), 'Y\r\nWJj' -> 'abc' (embedded CRLF).
statement
INSERT INTO test_unbase64 VALUES
  ('YWJj'),
  ('YQ=='),
  ('YWI='),
  (''),
  (NULL),
  ('YW Jj'),
  ('Y\r\nWJj')

query
SELECT s, hex(unbase64(s)) FROM test_unbase64

-- Literal arguments across the same shapes.
query
SELECT hex(unbase64('YWJj')),
       hex(unbase64('YQ==')),
       hex(unbase64('YWI=')),
       hex(unbase64('')),
       hex(unbase64('YW Jj')),
       hex(unbase64('Y\r\nWJj'))

-- Unpadded input is accepted by the MIME decoder at clean 4-char boundaries.
-- 'YQ' exits with 2 alphabet chars consumed (shift=6, emits 1 byte).
-- 'YWI' and 'YWJ' both exit with 3 alphabet chars (shift=0, emits 2 bytes). 'YWJ' has non-zero
-- low bits in the third char that MIME mode silently discards — the JDK decoder does not
-- strict-check them, and this row pins that behavior against the standard decoder.
query
SELECT hex(unbase64('YQ')),
       hex(unbase64('YWI')),
       hex(unbase64('YWJ'))

-- Multi-byte UTF-8 bytes are outside the base64 alphabet, so every byte of the codepoint is
-- skipped. 'YQ==é' decodes to 'a' just like 'YQ=='; the trailing 2-byte é (0xC3 0xA9) is treated
-- the same as any non-alphabet byte after padding. '€' (0xE2 0x82 0xAC) covers the 3-byte
-- codepoint case in the same skip loop. Pin non-ASCII skipping alongside the space and CRLF
-- cases already covered above.
query
SELECT hex(unbase64('YQ==é')),
       hex(unbase64('YW€Jj')),
       hex(unbase64('YéWJj'))

-- Round-trip against Comet's own base64 output. With the default
-- `spark.sql.chunkBase64String.enabled = true`, values longer than 57 raw bytes encode with CRLF
-- separators every 76 characters; unbase64 must skip those separators so the round-trip returns
-- the original bytes.
statement
CREATE TABLE test_unbase64_roundtrip(s string) USING parquet

statement
INSERT INTO test_unbase64_roundtrip VALUES
  ('abc'),
  ('hello'),
  (''),
  (NULL),
  ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
  ('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
  ('cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc')

query
SELECT s, cast(unbase64(base64(cast(s AS binary))) AS string) FROM test_unbase64_roundtrip

-- Regression for apache/datafusion-comet#5451: when unbase64 has a compound child, Spark's
-- generated code preserves the child's short-circuit semantics and never evaluates branches
-- that would otherwise raise. The native ScalarFunctionExpr path evaluates its argument
-- eagerly, so compound children stay on the JVM codegen dispatcher via CodegenDispatchFallback.
-- CASE WHEN is used here because its short-circuit is guaranteed across all supported Spark
-- versions; the row (NULL, 'A') exercises the skipped branch that would otherwise raise on the
-- unpadded 1-char input.
statement
CREATE TABLE test_unbase64_short_circuit(n string, bad string) USING parquet

statement
INSERT INTO test_unbase64_short_circuit VALUES (NULL, 'A'), ('X', 'YWJj')

query
SELECT hex(unbase64(case when n is null then null
                         else cast(unbase64(bad) as string) end))
FROM test_unbase64_short_circuit

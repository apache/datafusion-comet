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

-- LIMIT and first-match joins can skip malformed input even when their expressions would
-- throw if evaluated. Native batch evaluation and the JVM dispatcher must preserve those
-- operator boundaries, including after AQE replans a join. Malformed terminal Base64 units
-- throw regardless of ANSI mode, and disabling JVM dispatch still permits native decoding.
-- ConfigMatrix: spark.comet.exec.scalaUDF.codegen.enabled=false,true
-- ConfigMatrix: spark.sql.adaptive.enabled=false,true
-- ConfigMatrix: spark.sql.ansi.enabled=false,true
-- Config: spark.sql.shuffle.partitions=1

-- Parquet keeps the decoder inputs as attributes. A single file fixes the physical row order
-- so LIMIT consumes the valid first row and never reaches the malformed second row.
statement
CREATE TABLE test_unbase64_operator_limit USING parquet AS
SELECT /*+ COALESCE(1) */ bad FROM VALUES ('YWJj'), ('A') AS t(bad)

query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT hex(unbase64(bad)) FROM test_unbase64_operator_limit LIMIT 1

query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT bad FROM test_unbase64_operator_limit
WHERE unbase64(bad) <=> X'616263' LIMIT 1

-- When enabled, the JVM dispatcher evaluates this compound child over whole batches too.
query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT hex(unbase64(concat(bad, ''))) FROM test_unbase64_operator_limit LIMIT 1

-- Strict decoding also uses UnBase64 through the JVM dispatcher when enabled.
query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT hex(to_binary(bad, 'base64')) FROM test_unbase64_operator_limit LIMIT 1

-- Removing the mask, or making the malformed row the consumed row, must still throw.
query expect_error(Last unit does not have enough valid bits)
SELECT hex(unbase64(bad)) FROM test_unbase64_operator_limit

query expect_error(Last unit does not have enough valid bits)
SELECT hex(unbase64(bad)) FROM test_unbase64_operator_limit WHERE bad = 'A' LIMIT 1

statement
CREATE TABLE test_unbase64_operator_left USING parquet AS
SELECT 1 AS k, X'616262' AS expected

-- Spark visits the last inserted build row first. Its successful match must prevent the
-- malformed candidate from being decoded, for both semi and anti joins.
statement
CREATE TABLE test_unbase64_operator_right USING parquet AS
SELECT /*+ COALESCE(1) */ k, bad FROM VALUES (1, 'A'), (1, 'YWJj') AS t(k, bad)

query expect_fallback(unbase64 requires Spark evaluation in first-match join conditions)
SELECT /*+ BROADCAST(r) */ l.* FROM test_unbase64_operator_left l
LEFT SEMI JOIN test_unbase64_operator_right r
ON l.k = r.k AND unbase64(r.bad) > l.expected

query expect_fallback(unbase64 requires Spark evaluation in first-match join conditions)
SELECT /*+ BROADCAST(r) */ l.* FROM test_unbase64_operator_left l
LEFT ANTI JOIN test_unbase64_operator_right r
ON l.k = r.k AND unbase64(r.bad) > l.expected

-- Reversing the build rows removes the first-match mask and exposes the malformed input.
statement
CREATE TABLE test_unbase64_operator_reverse USING parquet AS
SELECT /*+ COALESCE(1) */ k, bad FROM VALUES (1, 'YWJj'), (1, 'A') AS t(k, bad)

query expect_error(Last unit does not have enough valid bits)
SELECT /*+ BROADCAST(r) */ l.* FROM test_unbase64_operator_left l
LEFT SEMI JOIN test_unbase64_operator_reverse r
ON l.k = r.k AND unbase64(r.bad) > l.expected

query expect_error(Last unit does not have enough valid bits)
SELECT /*+ BROADCAST(r) */ l.* FROM test_unbase64_operator_left l
LEFT ANTI JOIN test_unbase64_operator_reverse r
ON l.k = r.k AND unbase64(r.bad) > l.expected

-- Safe projections and inner joins must retain native decoding, including nullable input.
statement
CREATE TABLE test_unbase64_operator_valid USING parquet AS
SELECT /*+ COALESCE(1) */ k, bad FROM VALUES (1, 'YWJj'), (1, 'YWFh'), (1, NULL) AS t(k, bad)

-- Falling back for a decoded group key must also keep the partial collect_list in Spark,
-- because its intermediate buffer cannot be shared with a native partial aggregate.
-- Select one valid group so LIMIT cannot choose different groups in Spark and Comet.
query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT hex(unbase64(bad)), sort_array(collect_list(k))
FROM test_unbase64_operator_valid WHERE bad = 'YWJj'
GROUP BY bad LIMIT 1

-- An offset-only collect does not stop input early. With one row, AQE can remove the sort
-- after the native partial collect_list has materialized; the final must remain native.
statement
CREATE TABLE test_unbase64_operator_offset USING parquet AS
SELECT 1 AS k, 'YWJj' AS bad

query
SELECT unbase64(bad) AS decoded, collect_list(k)
FROM test_unbase64_operator_offset
GROUP BY bad ORDER BY decoded OFFSET 1

query
SELECT hex(unbase64(bad)) FROM test_unbase64_operator_valid

query
SELECT /*+ BROADCAST(r) */ l.* FROM test_unbase64_operator_left l
INNER JOIN test_unbase64_operator_valid r
ON l.k = r.k AND unbase64(r.bad) > l.expected

-- Semi/anti joins without a throwing decoder remain native.
query
SELECT /*+ BROADCAST(r) */ l.* FROM test_unbase64_operator_left l
LEFT SEMI JOIN test_unbase64_operator_right r ON l.k = r.k

query
SELECT /*+ BROADCAST(r) */ l.* FROM test_unbase64_operator_left l
LEFT ANTI JOIN test_unbase64_operator_right r ON l.k = r.k

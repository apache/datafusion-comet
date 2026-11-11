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

statement
CREATE TABLE test_signum(d double) USING parquet

statement
INSERT INTO test_signum VALUES (5.0), (-5.0), (0.0), (NULL), (cast('NaN' as double)), (cast('Infinity' as double)), (cast('-Infinity' as double))

query
SELECT signum(d) FROM test_signum

-- literal arguments
query
SELECT signum(-5.0), signum(5.0), signum(0.0), signum(-0.0), signum(NULL)

-- java.lang.Math.signum returns the zero it is given, so -0.0 keeps its sign
-- (https://github.com/apache/datafusion-comet/issues/6522)
statement
CREATE TABLE test_signum_zero(d double, f float) USING parquet

statement
INSERT INTO test_signum_zero VALUES (double('-0.0'), float('-0.0')), (double('0.0'), float('0.0')), (-1.5, float('-1.5')), (2.5, float('2.5')), (NULL, NULL)

query
SELECT signum(d), signum(f) FROM test_signum_zero

-- the string form shows the sign of a zero
query
SELECT CAST(signum(d) AS STRING), CAST(signum(f) AS STRING) FROM test_signum_zero

-- Spark's Signum also accepts the two interval types. The native kernel takes doubles only, so
-- these fall back.
statement
CREATE TABLE test_signum_iv(m int) USING parquet

statement
INSERT INTO test_signum_iv VALUES (18), (-18), (0), (NULL)

query expect_fallback(signum does not support input type)
SELECT signum(make_ym_interval(0, m)), signum(make_dt_interval(m, 0, 0, 0)) FROM test_signum_iv

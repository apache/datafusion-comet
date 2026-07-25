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

-- pow runs natively and matches Spark exactly, including edge cases such as pow(0, -1) = Infinity.

statement
CREATE TABLE test_pow(base double, exp double) USING parquet

-- Rows cover: ordinary values, nulls, signed zero, positive/negative infinity in both base and
-- exponent, subnormal values, and the |base| == 1 with non-finite exponent case where Java's
-- Math.pow returns NaN but C99 pow (Rust powf) returns 1. Every result is an exact double
-- (Infinity, -Infinity, NaN, signed zero, or an exact power), so these are compared exactly
-- rather than with a tolerance, which for doubles skips NaN rows and ignores the sign of Infinity.
statement
INSERT INTO test_pow VALUES
  (0.0, -1.0), (2.0, 3.0), (0.0, 0.0), (-1.0, 2.0), (-1.0, 0.5), (2.0, -1.0),
  (NULL, 2.0), (2.0, NULL),
  (cast('NaN' as double), 2.0), (cast('Infinity' as double), 2.0), (2.0, cast('Infinity' as double)),
  (cast('-0.0' as double), -1.0), (cast('-0.0' as double), -2.0), (cast('-0.0' as double), 3.0), (cast('-0.0' as double), 2.0),
  (cast('-Infinity' as double), 2.0), (cast('-Infinity' as double), 3.0), (cast('-Infinity' as double), -1.0),
  (2.0, cast('-Infinity' as double)), (0.5, cast('-Infinity' as double)),
  (1.0, cast('Infinity' as double)), (-1.0, cast('Infinity' as double)), (1.0, cast('-Infinity' as double)),
  (1.0, cast('NaN' as double)), (-1.0, cast('NaN' as double)),
  (cast('4.9E-324' as double), 2.0), (2.0, cast('4.9E-324' as double))

-- Every pair above yields an exact double, so use exact comparison (no tolerance) to assert the
-- NaN and signed-Infinity edge cases rather than silently skipping them.
query
SELECT pow(base, exp) FROM test_pow

-- column + literal
query tolerance=1e-6
SELECT pow(base, 2.0) FROM test_pow

-- literal + column
query tolerance=1e-6
SELECT pow(2.0, exp) FROM test_pow

-- literal + literal
query tolerance=1e-6
SELECT pow(2.0, 3.0), pow(0.0, 0.0), pow(-1.0, 2.0), pow(NULL, 2.0)

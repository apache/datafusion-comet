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

-- Spark's ArrayUnion is a BinaryExpression: for a NULL left array it returns NULL without
-- evaluating the right operand, so a right operand that would throw on that row doesn't. Comet
-- guards the native call with CASE WHEN <left> IS NOT NULL to match.
-- https://github.com/apache/datafusion-comet/issues/6613

statement
CREATE TABLE array_union_null_left(a array<int>, b array<int>, da array<double>, db array<double>) USING parquet

statement
INSERT INTO array_union_null_left VALUES (NULL, array(1), NULL, array(1.0D))

-- slice with a start of 0 throws whenever it is evaluated, in any mode
query
SELECT array_union(a, slice(b, 0, 1)) FROM array_union_null_left

-- Float elements run natively only on the Spark versions whose floats Comet can match, so the
-- path depends on the version
query spark_answer_only
SELECT array_union(da, slice(db, 0, 1)) FROM array_union_null_left

-- The guard would evaluate a nondeterministic left operand twice, so that shape stays on Spark
query expect_fallback(nullable nondeterministic left operand)
SELECT array_union(IF(rand(42) > 0.5D, a, NULL), slice(b, 1, 1)) FROM array_union_null_left

statement
CREATE TABLE array_union_mixed(a array<int>, b array<int>) USING parquet

statement
INSERT INTO array_union_mixed VALUES (NULL, array(1)), (array(2), array(1)), (array(3), NULL)

-- The guard selects only some rows of the batch
query
SELECT array_union(a, slice(b, 1, 1)) FROM array_union_mixed

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

-- MaxSparkVersion: 3.5

-- Spark 3 keeps positive and negative zero distinct; these cases can be removed with Spark 3 support.

statement
CREATE TABLE test_array_set_signed_zero_float(a array<float>, b array<float>) USING parquet

statement
INSERT INTO test_array_set_signed_zero_float VALUES
  (array(float('0.0'), float('-0.0'), float('1.0')), array(float('0.0'))),
  (array(float('0.0'), float('1.0')), array(float('-0.0'))),
  (array(float('-0.0')), array(float('0.0'))),
  (array(float('0.0')), array(float('-0.0'))),
  (array(float('-0.0')), array(float('-0.0'))),
  (array(float('0.0'), float('-0.0')), array(float('0.0')))

query expect_fallback(SPARK-54918)
SELECT array_distinct(array(float('0.0'), float('-0.0'), float('1.0')))

query expect_fallback(SPARK-54918)
SELECT array_union(array(float('0.0')), array(float('-0.0')))

query expect_fallback(SPARK-54918)
SELECT a, b, array_distinct(a) FROM test_array_set_signed_zero_float

query expect_fallback(SPARK-54918)
SELECT a, b, array_union(a, b) FROM test_array_set_signed_zero_float

query
SELECT a, b, array_except(a, b) FROM test_array_set_signed_zero_float

query
SELECT a, b, array_intersect(a, b) FROM test_array_set_signed_zero_float

statement
CREATE TABLE test_array_set_signed_zero_double(a array<double>, b array<double>) USING parquet

statement
INSERT INTO test_array_set_signed_zero_double VALUES
  (array(double('0.0'), double('-0.0'), double('1.0')), array(double('0.0'))),
  (array(double('0.0'), double('1.0')), array(double('-0.0'))),
  (array(double('-0.0')), array(double('0.0'))),
  (array(double('0.0')), array(double('-0.0'))),
  (array(double('-0.0')), array(double('-0.0'))),
  (array(double('0.0'), double('-0.0')), array(double('0.0')))

query expect_fallback(SPARK-54918)
SELECT array_distinct(array(double('0.0'), double('-0.0'), double('1.0')))

query expect_fallback(SPARK-54918)
SELECT array_union(array(double('0.0')), array(double('-0.0')))

query expect_fallback(SPARK-54918)
SELECT a, b, array_distinct(a) FROM test_array_set_signed_zero_double

query expect_fallback(SPARK-54918)
SELECT a, b, array_union(a, b) FROM test_array_set_signed_zero_double

query
SELECT a, b, array_except(a, b) FROM test_array_set_signed_zero_double

query
SELECT a, b, array_intersect(a, b) FROM test_array_set_signed_zero_double

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

-- ArrayIntersect mixes in CodegenDispatchFallback, so with allowIncompatible unset its
-- Incompatible element-order case routes through the JVM codegen dispatcher and matches Spark
-- exactly, including the right-longer-than-left case the native path orders differently (no
-- sort_array workaround needed here).

statement
CREATE TABLE test_ai_dispatch(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_ai_dispatch VALUES (array(2, 1), array(3, 1, 2)), (array(3, 1), array(1, 2, 3, 4)), (array(1, NULL), array(NULL, 2)), (NULL, array(1))

query
SELECT array_intersect(a, b) FROM test_ai_dispatch

query
SELECT array_intersect(array(2, 1), array(3, 1, 2)), array_intersect(array(3, 1), array(1, 2, 3, 4))

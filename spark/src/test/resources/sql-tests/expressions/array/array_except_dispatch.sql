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

-- ArrayExcept mixes in CodegenDispatchFallback, so with allowIncompatible unset its Incompatible
-- null-handling/ordering case routes through the JVM codegen dispatcher and matches Spark exactly,
-- including the literal/literal case the native path could not handle.

statement
CREATE TABLE test_ae_dispatch(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_ae_dispatch VALUES (array(1, 2, 3), array(2, 3, 4)), (array(1, 2), array()), (array(), array(1)), (NULL, array(1)), (array(1, NULL), array(NULL))

query
SELECT array_except(a, b) FROM test_ae_dispatch

query
SELECT array_except(array(1, 2, 3), array(2, 3, 4)), array_except(array(1, 2), array()), array_except(array(), array(1)), array_except(cast(NULL as array<int>), array(1))

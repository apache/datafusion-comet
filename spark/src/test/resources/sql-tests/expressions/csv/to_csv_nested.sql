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

-- Nested array/map/struct fields have no native to_csv path and stay on the JVM codegen
-- dispatcher. Spark 3.4/3.5 stringify nested arrays via Object.toString(); Spark 4.0
-- (SPARK-47497) prints pretty strings that match the dispatcher.

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_to_csv_nested(id INT, items ARRAY<INT>) USING parquet

statement
INSERT INTO test_to_csv_nested VALUES (1, array(1, 2)), (2, array()), (3, NULL)

query
SELECT to_csv(named_struct('id', id, 'items', items)) FROM test_to_csv_nested

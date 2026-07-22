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

-- ConfigMatrix: spark.sql.legacy.sizeOfNull=true,false

statement
CREATE TABLE test_size(arr array<int>, m map<string, int>) USING parquet

statement
INSERT INTO test_size VALUES (array(1, 2, 3), map('a', 1, 'b', 2)), (array(), map()), (NULL, NULL)

query
SELECT size(arr), size(m) FROM test_size

-- literal array arguments
query
SELECT size(array(1, 2, 3)), size(array()), size(cast(NULL as array<int>))

-- literal map via CreateMap (falls back: Comet has no CreateMap serde;
-- cast(NULL as map) avoids CreateMap and goes through CometLiteral instead)
query spark_answer_only
SELECT size(map('a', 1, 'b', 2)), size(map())

query
SELECT size(cast(NULL as map<string,int>))

-- cardinality is a SQL alias for size
query
SELECT cardinality(arr), cardinality(m) FROM test_size

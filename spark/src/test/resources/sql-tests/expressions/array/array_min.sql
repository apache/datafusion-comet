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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_array_min(arr array<int>) USING parquet

statement
INSERT INTO test_array_min VALUES (array(1, 2, 3)), (array(3, 1, 2)), (array()), (NULL), (array(NULL, 1, 2)), (array(-1, -2, -3))

query spark_answer_only
SELECT array_min(arr) FROM test_array_min

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3338)
SELECT array_min(array(1, 2, 3)), array_min(array()), array_min(cast(NULL as array<int>))

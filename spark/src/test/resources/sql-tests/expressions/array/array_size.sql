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

-- array_size returns NULL for NULL input (unlike size which returns -1 in legacy mode).

statement
CREATE TABLE test_array_size(arr array<int>) USING parquet

statement
INSERT INTO test_array_size VALUES (array(1, 2, 3)), (array(10)), (array()), (NULL)

-- non-null arrays
query
SELECT array_size(arr) FROM test_array_size WHERE arr IS NOT NULL

-- literal arguments: non-null
query
SELECT array_size(array(1, 2, 3)), array_size(array(10)), array_size(array())

-- NULL input: Spark returns NULL; Comet bug returns -1
-- tracked in https://github.com/apache/datafusion-comet/issues/4560
query ignore(https://github.com/apache/datafusion-comet/issues/4560)
SELECT array_size(CAST(NULL AS ARRAY<INT>))

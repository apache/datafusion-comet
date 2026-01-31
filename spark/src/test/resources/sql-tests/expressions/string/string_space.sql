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
CREATE TABLE test_space(n int) USING parquet

statement
INSERT INTO test_space VALUES (0), (1), (5), (NULL), (-1)

query
SELECT concat('[', space(n), ']') FROM test_space WHERE n >= 0 OR n IS NULL

-- Comet bug: space(-1) causes native crash "failed to round upto multiple of 64"
-- https://github.com/apache/datafusion-comet/issues/3326
query ignore(https://github.com/apache/datafusion-comet/issues/3326)
SELECT concat('[', space(n), ']') FROM test_space WHERE n < 0

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3337)
SELECT concat('[', space(5), ']'), concat('[', space(0), ']'), space(-1), space(NULL)

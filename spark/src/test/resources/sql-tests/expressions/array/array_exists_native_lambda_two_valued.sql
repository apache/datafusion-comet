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

-- With `spark.sql.legacy.followThreeValuedLogicInArrayExists=false` Spark returns false where it
-- would return NULL because the predicate was NULL for some element and false for the rest.
-- DataFusion's `array_any_match` returns NULL there, so `exists` stays on the dispatcher.

-- Config: spark.sql.legacy.followThreeValuedLogicInArrayExists=false

statement
CREATE TABLE test_exists_two_valued(id int, ints array<int>) USING parquet

statement
INSERT INTO test_exists_two_valued VALUES (1, array(1, NULL, 3)), (2, array()), (3, NULL), (4, array(NULL, NULL))

query expect_dispatch(exists)
SELECT id, exists(ints, x -> x > 2), exists(ints, x -> x > 10) FROM test_exists_two_valued

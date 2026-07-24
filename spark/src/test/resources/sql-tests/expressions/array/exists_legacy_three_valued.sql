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

-- Regression coverage for `spark.sql.legacy.followThreeValuedLogicInArrayExists`. Spark's
-- `ArrayExists` captures the flag into a boolean field at construction time
-- (`ArrayExists.followThreeValuedLogic`). `CometArrayExists` is a `CometCodegenDispatch`, so
-- Spark's own `doGenCode` runs inside the Comet kernel and closes over that field. Both flag
-- values therefore produce Spark-exact results without any serde-level gate.
--
-- The config's default is `true` in all Comet-supported Spark versions (3.4.3, 3.5.8, 4.0.1,
-- 4.1.1). Under `true`, a predicate that yields NULL for some element and no TRUE elsewhere
-- makes `exists` return NULL. Under `false`, NULL predicate results are treated as FALSE.

-- ConfigMatrix: spark.sql.legacy.followThreeValuedLogicInArrayExists=true,false
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_exists_3vl(a array<int>) USING parquet

statement
INSERT INTO test_exists_3vl VALUES
  (array(1, 2, 3)),
  (array(1, NULL, 3)),
  (array(NULL, NULL)),
  (array()),
  (NULL)

-- Non-null array with a matching element: always true regardless of flag.
query
SELECT exists(a, x -> x > 2) FROM test_exists_3vl

-- Predicate never true for non-null elements. Under legacy=true a null element makes the
-- result NULL; under legacy=false the null is treated as false so the result is false.
query
SELECT exists(a, x -> x > 10) FROM test_exists_3vl

-- Predicate explicitly handles NULL (returns TRUE for null elements) — result is
-- independent of the flag.
query
SELECT exists(a, x -> x IS NULL) FROM test_exists_3vl

-- Predicate that is null-safe (COALESCE) — predicate never returns NULL, so both flag values
-- produce the same TRUE/FALSE result.
query
SELECT exists(a, x -> coalesce(x, -1) > 0) FROM test_exists_3vl

-- Literal-array cases pinning each corner:
--   * a matching value is found -> true under both configs
--   * no match, but a null element present -> null (legacy=true) / false (legacy=false)
--   * empty array -> false under both configs
--   * NULL array -> null under both configs
query
SELECT
  exists(array(1, NULL, 3), x -> x > 2),
  exists(array(1, NULL, 3), x -> x > 10),
  exists(array(NULL, NULL), x -> x > 0),
  exists(array(), x -> x > 0),
  exists(cast(NULL as array<int>), x -> x > 0)

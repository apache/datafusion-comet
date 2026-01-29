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
CREATE TABLE test_arrays_overlap(a array<int>, b array<int>) USING parquet

statement
INSERT INTO test_arrays_overlap VALUES (array(1, 2, 3), array(3, 4, 5)), (array(1, 2), array(3, 4)), (array(), array(1)), (NULL, array(1)), (array(1, NULL), array(NULL, 2))

query spark_answer_only
SELECT arrays_overlap(a, b) FROM test_arrays_overlap

-- column + literal
query spark_answer_only
SELECT arrays_overlap(a, array(3, 4, 5)) FROM test_arrays_overlap

-- literal + column
query spark_answer_only
SELECT arrays_overlap(array(1, 2, 3), b) FROM test_arrays_overlap

-- literal + literal
query ignore(https://github.com/apache/datafusion-comet/issues/3338)
SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5)), arrays_overlap(array(1, 2), array(3, 4)), arrays_overlap(array(), array(1)), arrays_overlap(cast(NULL as array<int>), array(1))

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

-- Without Comet shuffle, a sort aggregate's Partial and Final would be split across Comet and
-- Spark, and Spark cannot read Comet's collect_set buffer, so SortAggregateExec stays in Spark.
-- Config: spark.sql.execution.useObjectHashAggregateExec=false
-- Config: spark.comet.shuffle.enabled=false

statement
CREATE TABLE sa_no_shuffle(i int, g string) USING parquet

statement
INSERT INTO sa_no_shuffle VALUES (1, 'a'), (2, 'a'), (1, 'a'), (3, 'b'), (NULL, 'b')

query expect_fallback(Comet shuffle is not enabled, so converting SortAggregate would split the aggregate across Comet and Spark)
SELECT g, sort_array(collect_set(i)) FROM sa_no_shuffle GROUP BY g ORDER BY g

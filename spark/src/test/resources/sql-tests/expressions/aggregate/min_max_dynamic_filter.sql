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

-- ConfigMatrix: spark.comet.exec.aggregate.dynamicFilter.enabled=false,true

statement
CREATE TABLE min_max_dynamic_filter(k int) USING parquet

query
SELECT MIN(k) FROM min_max_dynamic_filter

query
SELECT MAX(k) FROM min_max_dynamic_filter

statement
INSERT INTO min_max_dynamic_filter VALUES (NULL), (NULL)

query
SELECT MIN(k) FROM min_max_dynamic_filter

query
SELECT MAX(k) FROM min_max_dynamic_filter

statement
INSERT INTO min_max_dynamic_filter VALUES (-2), (-2), (3)

query
SELECT MIN(k) FROM min_max_dynamic_filter

query
SELECT MAX(k) FROM min_max_dynamic_filter

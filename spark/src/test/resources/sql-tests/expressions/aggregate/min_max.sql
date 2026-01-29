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
CREATE TABLE test_min_max(i int, d double, s string, grp string) USING parquet

statement
INSERT INTO test_min_max VALUES (1, 1.5, 'b', 'x'), (3, 3.5, 'a', 'x'), (2, 2.5, 'c', 'y'), (NULL, NULL, NULL, 'y'), (-1, -1.5, 'z', 'x')

query expect_fallback(SortAggregate is not supported)
SELECT min(i), max(i), min(d), max(d), min(s), max(s) FROM test_min_max

query
SELECT grp, min(i), max(i) FROM test_min_max GROUP BY grp ORDER BY grp

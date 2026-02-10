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
CREATE TABLE test_sum(i int, l long, f float, d double, grp string) USING parquet

statement
INSERT INTO test_sum VALUES (1, 10, 1.5, 1.5, 'a'), (2, 20, 2.5, 2.5, 'a'), (3, 30, 3.5, 3.5, 'b'), (NULL, NULL, NULL, NULL, 'b'), (2147483647, 9223372036854775807, cast('Infinity' as float), cast('Infinity' as double), 'c')

query
SELECT sum(i), sum(l) FROM test_sum

query tolerance=1e-6
SELECT sum(f), sum(d) FROM test_sum

query
SELECT grp, sum(i) FROM test_sum GROUP BY grp ORDER BY grp

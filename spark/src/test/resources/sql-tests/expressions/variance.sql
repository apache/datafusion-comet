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
CREATE TABLE test_variance(d double, grp string) USING parquet

statement
INSERT INTO test_variance VALUES (1.0, 'a'), (2.0, 'a'), (3.0, 'a'), (4.0, 'b'), (5.0, 'b'), (NULL, 'b')

query tolerance=1e-6
SELECT variance(d), var_samp(d), var_pop(d) FROM test_variance

query tolerance=1e-6
SELECT grp, variance(d) FROM test_variance GROUP BY grp ORDER BY grp

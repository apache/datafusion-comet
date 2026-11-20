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

-- Queries without a FROM clause run over a OneRowRelation, which Comet converts by default.

-- A folded array literal is an UnsafeArrayData, not a GenericArrayData.
query
SELECT sequence(1, 5)

query
SELECT sequence(1, 5), array(array(1, 2), array(3)), array('a', NULL, 'c')

query
SELECT explode(sequence(1, 3))

-- GROUPING SETS over a single row.
statement
CREATE TEMPORARY VIEW one_row_user_view AS SELECT current_user

query
SELECT count(*) FROM one_row_user_view GROUP BY current_user GROUPING SETS ((current_user))

query
SELECT current_user, grouping(current_user) FROM one_row_user_view GROUP BY ROLLUP(current_user)

query
SELECT count(*) FROM (SELECT 1 AS a) GROUP BY GROUPING SETS ((a), ())

statement
DROP VIEW one_row_user_view

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

-- IN builds its list once when every candidate is a constant, which it decides by evaluating the
-- candidates on an empty batch and checking for a scalar. A CASE or IF that depends on a column
-- must not return a scalar NULL there, or IN compares every row against NULL.

-- Config: spark.comet.exec.range.enabled=true
-- Config: spark.comet.sparkToColumnar.enabled=true
-- Config: spark.comet.sparkToColumnar.supportedOperatorList=Range

query
SELECT id, id IN (IF(id = 1, NULL, id)), id IN (nullif(id, 1), 5L) FROM range(0, 3)

query
SELECT id, id IN (CASE WHEN id = 1 THEN NULL ELSE id END) FROM range(0, 3)

-- no ELSE
query
SELECT id, id IN (CASE WHEN id <> 1 THEN id END), id NOT IN (CASE WHEN id <> 1 THEN id END)
FROM range(0, 3)

-- nested operands
query
SELECT id, named_struct('a', id) IN (named_struct('a', IF(id = 1, NULL, id))) FROM range(0, 3)

query
SELECT id, named_struct('a', CAST(id AS DOUBLE))
  IN (named_struct('a', IF(id = 1, NULL, CAST(id AS DOUBLE))))
FROM range(0, 3)

query
SELECT id, array(CAST(id AS DOUBLE)) IN (array(IF(id = 1, NULL, CAST(id AS DOUBLE))))
FROM range(0, 3)

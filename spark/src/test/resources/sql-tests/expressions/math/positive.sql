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

-- Routes positive (unary plus) through the codegen dispatcher so behavior matches Spark exactly.

statement
CREATE TABLE test_positive(v int) USING parquet

statement
INSERT INTO test_positive VALUES (5), (-3), (0), (NULL)

query
SELECT v, positive(v) FROM test_positive

-- literal arguments
query
SELECT positive(5), positive(-3.2), positive(0)

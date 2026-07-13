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

-- mask routed through the codegen dispatcher.

-- MinSparkVersion: 3.5

statement
CREATE TABLE test_mask(s string) USING parquet

statement
INSERT INTO test_mask VALUES ('AbCD123-$#'), ('Spark'), (''), (NULL)

query
SELECT mask(s), s FROM test_mask

query
SELECT mask('AbCD123-$#', 'Q', 'q', 'd', 'o'), mask('Spark', 'X')

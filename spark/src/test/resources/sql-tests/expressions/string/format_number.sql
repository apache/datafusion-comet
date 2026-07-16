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

-- Routes format_number through the codegen dispatcher so behavior matches Spark exactly.

statement
CREATE TABLE test_format_number(v double) USING parquet

statement
INSERT INTO test_format_number VALUES (12345.6789), (0.0), (-9876.54321), (NULL)

query
SELECT v, format_number(v, 2) FROM test_format_number

-- literal arguments
query
SELECT format_number(12345.6789, 2), format_number(12345.6789, 0), format_number(5, 4)

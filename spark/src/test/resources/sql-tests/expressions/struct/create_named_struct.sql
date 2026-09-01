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

statement
CREATE TABLE test_named_struct(a int, b string, c double) USING parquet

statement
INSERT INTO test_named_struct VALUES (1, 'hello', 1.5), (NULL, NULL, NULL), (0, '', 0.0)

query
SELECT named_struct('x', a, 'y', b, 'z', c) FROM test_named_struct

query
SELECT struct(a, b, c) FROM test_named_struct

-- literal arguments
query
SELECT named_struct('x', 1, 'y', 'hello', 'z', 3.14)

query
SELECT named_struct('x', a, 'y', 'fixed_val', 'z', c) FROM test_named_struct

-- duplicate names dispatch through Spark codegen while preserving ordinal values
query
SELECT named_struct('x', a, 'x', b) FROM test_named_struct

-- struct() lowers to CreateNamedStruct and derives duplicate names from repeated children
query
SELECT struct(a, a) FROM test_named_struct

-- nested duplicate-name structs exercise list and map roots during Arrow import
query
SELECT array(named_struct('x', a, 'x', b)) FROM test_named_struct

query
SELECT map('row', named_struct('x', a, 'x', b)) FROM test_named_struct

-- nested structs, three duplicates, and an all-null row
query
SELECT named_struct('outer', named_struct('x', a, 'x', b)) FROM test_named_struct

query
SELECT named_struct('x', a, 'x', b, 'x', c) FROM test_named_struct

query
SELECT named_struct('x', a, 'x', b, 'x', c) FROM test_named_struct WHERE a IS NULL

-- construct the duplicate-name struct after a supported primitive-key shuffle boundary
query
SELECT named_struct('x', a, 'x', b)
FROM (SELECT /*+ REPARTITION(2, a) */ a, b FROM test_named_struct) shuffled

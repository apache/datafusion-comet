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

-- uuid() runs natively. The raw random value cannot be compared against Spark because Spark assigns
-- a fresh random seed on each planning pass, so these queries assert deterministic properties that
-- hold identically on both engines. The seeded uuid(seed) form (Spark 4.0+) asserts bit-for-bit
-- equality with Spark and lives in uuid_with_seed.sql.

statement
CREATE TABLE test_uuid(id int) USING parquet

statement
INSERT INTO test_uuid VALUES (1), (2), (3), (4), (5)

-- canonical form is 36 characters
query
SELECT length(uuid()) FROM test_uuid

-- matches the RFC 4122 version 4 layout (version nibble 4, variant nibble 8/9/a/b), lowercase hex.
-- This regex subsumes the length, alphabet, version, and variant checks.
query
SELECT uuid() RLIKE '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' FROM test_uuid

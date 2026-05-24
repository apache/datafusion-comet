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

-- schema_of_json requires a foldable (literal) JSON argument; its result is a
-- constant schema string. In production, ConstantFolding collapses it to a
-- string literal that runs natively in Comet. This test suite disables
-- ConstantFolding, so the expression survives to the physical plan and Comet
-- falls back. The exact fallback reason differs by Spark version:
--   * Spark 3.4 / 3.5: "schema_of_json is not supported" (plain expression)
--   * Spark 4.0+:      "invoke is not supported" (SchemaOfJson is now
--                       RuntimeReplaceable -> Invoke on SchemaOfJsonEvaluator)
-- so we assert on the common substring "is not supported". These assertions
-- document the current fallback behavior while still verifying Comet's result
-- matches Spark. If Comet later runs this natively, flip to plain `query`.

query expect_fallback(is not supported)
SELECT schema_of_json('{"name":"John", "age":30}')

query expect_fallback(is not supported)
SELECT schema_of_json('{"users":[{"name":"John","scores":[95,87]}]}')

query expect_fallback(is not supported)
SELECT schema_of_json('[1, 2, 3]')

query expect_fallback(is not supported)
SELECT schema_of_json('{}')

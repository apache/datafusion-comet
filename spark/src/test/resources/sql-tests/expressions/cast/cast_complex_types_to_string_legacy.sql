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

-- When `spark.sql.legacy.castComplexTypesToString.enabled` is true Spark wraps maps and
-- structs with `[...]` (instead of `{...}`) and omits NULL elements of structs/maps/arrays
-- (instead of rendering them as the literal "null"). Comet's native cast does not implement
-- the legacy formatting; the [[CodegenDispatchFallback]] mixin on `CometCast` routes these
-- casts through the JVM codegen dispatcher (Spark's own `doGenCode` inside the Comet kernel)
-- so results match Spark exactly without a Spark fallback.
-- The flag is internal in Spark 4.0 and defaults to false.

-- Config: spark.sql.legacy.castComplexTypesToString.enabled=true

-- Struct → string routed through the codegen dispatcher.
query
SELECT CAST(struct(1, 2, null) AS STRING)

-- Array → string routed through the codegen dispatcher.
query
SELECT CAST(array(1, 2, null) AS STRING)

-- Map → string routed through the codegen dispatcher.
query
SELECT CAST(map('a', 1, 'b', null) AS STRING)

-- Nested complex types also routed through the codegen dispatcher via the outer type.
query
SELECT CAST(struct(array(1, null), map('k', null)) AS STRING)

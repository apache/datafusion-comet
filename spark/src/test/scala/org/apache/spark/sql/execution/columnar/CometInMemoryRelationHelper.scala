/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.execution.columnar

/**
 * Test-only access to `InMemoryRelation`'s JVM-wide cached `CachedBatchSerializer`.
 *
 * `InMemoryRelation` resolves `spark.sql.cache.serializer` once per JVM and memoizes the instance
 * in a static field, so the first suite in a forked JVM that caches a table pins the serializer
 * for every suite that follows. A suite that needs a specific cache serializer must reset that
 * state around itself. `InMemoryRelation.clearSerializer` is `private[columnar]`, hence this
 * shim.
 */
object CometInMemoryRelationHelper {
  def clearSerializer(): Unit = InMemoryRelation.clearSerializer()
}

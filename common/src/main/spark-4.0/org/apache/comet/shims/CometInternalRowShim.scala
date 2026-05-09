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

package org.apache.comet.shims

import org.apache.spark.unsafe.types.VariantVal

/**
 * Throwing-default implementations for `SpecializedGetters` methods that were added in Spark 4.0:
 * `getVariant`. The Janino-generated kernel subclasses `CometInternalRow` (rows) and
 * `CometArrayData` (array inputs), and each must satisfy every abstract method on the interface;
 * without these defaults the compiled class fails its abstract-method check at class-load time.
 * `GeographyVal` and `GeometryVal` were added in 4.1, so this profile's shim does not override
 * those getters.
 */
trait CometInternalRowShim {
  def getVariant(ordinal: Int): VariantVal =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: getVariant not supported")
}

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

package org.apache.comet.codegen

import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}

/**
 * Shim base for Comet-owned [[MapData]] views used by the Arrow-direct codegen kernel. Provides
 * `UnsupportedOperationException` defaults for every abstract method on `MapData`; the codegen-
 * emitted `InputMap_${path}` subclass overrides `numElements`, `keyArray`, and `valueArray`.
 *
 * Pairs with [[CometArrayData]] and [[CometInternalRow]]. `MapData` does not extend
 * `SpecializedGetters` (unlike `ArrayData` / `InternalRow`), so no version-specific shim is
 * needed here.
 */
abstract class CometMapData extends MapData {

  override def keyArray(): ArrayData = unsupported("keyArray")

  override def valueArray(): ArrayData = unsupported("valueArray")

  override def copy(): MapData = unsupported("copy")

  protected def unsupported(method: String): Nothing =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: $method not implemented for this map shape")

  override def toString(): String = {
    val n =
      try numElements().toString
      catch {
        case _: Throwable => "?"
      }
    s"${getClass.getSimpleName}(numElements=$n)"
  }

  override def numElements(): Int = unsupported("numElements")
}

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

import org.apache.spark.unsafe.types.{GeographyVal, GeometryVal, VariantVal}

/**
 * Throwing defaults for Spark 4.x `SpecializedGetters` additions: `getVariant` (4.0),
 * `getGeography` and `getGeometry` (4.1). Mixed into `CometInternalRow` and `CometArrayData`.
 */
trait CometInternalRowShim {
  def getVariant(ordinal: Int): VariantVal =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: getVariant not supported")

  def getGeography(ordinal: Int): GeographyVal =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: getGeography not supported")

  def getGeometry(ordinal: Int): GeometryVal =
    throw new UnsupportedOperationException(
      s"${getClass.getSimpleName}: getGeometry not supported")
}

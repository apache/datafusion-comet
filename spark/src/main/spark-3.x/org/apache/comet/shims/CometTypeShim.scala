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

import scala.annotation.nowarn

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

trait CometTypeShim {
  @nowarn // Spark 4 feature; stubbed to false in Spark 3.x for compatibility.
  def isStringCollationType(dt: DataType): Boolean = false

  @nowarn // Spark 4 feature; stubbed to false in Spark 3.x for compatibility.
  def hasNonDefaultStringCollation(dt: DataType): Boolean = false

  @nowarn // Spark 4 feature; collation does not exist in Spark 3.x.
  def hasCollationSupport: Boolean = false

  @nowarn // Spark 4 feature; Variant shredding doesn't exist in Spark 3.x.
  def isVariantStruct(s: StructType): Boolean = false

  @nowarn // Spark 4 feature; VariantType doesn't exist in Spark 3.x.
  def isVariantType(dt: DataType): Boolean = false

  @nowarn // Spark 4.1 feature; TimeType doesn't exist in Spark 3.x.
  def isTimeType(dt: DataType): Boolean = false

  /**
   * Compare two strings under the collation of `dt`, which must be a `StringType`.
   *
   * Spark 3.x has no collations, so every string comparison is byte order. Callers that record
   * comparable bounds (Comet's cache statistics, for instance) use this so the ordering they
   * store is the one Spark's own comparison would produce.
   */
  @nowarn // Collation is a Spark 4 feature; on 3.x every StringType compares as bytes.
  def compareStrings(left: UTF8String, right: UTF8String, dt: DataType): Int =
    ByteArray.compareBinary(left.getBytes, right.getBytes)
}

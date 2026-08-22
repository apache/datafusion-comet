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

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, Expression, Literal}
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructType, VariantType}
import org.apache.spark.unsafe.types.VariantVal

trait CometTypeShim {
  // A `StringType` carries collation metadata in Spark 4.0. Only non-default (non-UTF8_BINARY)
  // collations have semantics Comet's byte-level hashing/sorting/equality cannot honor. The
  // default `StringType` object is `StringType(UTF8_BINARY_COLLATION_ID)`, so comparing
  // `collationId` against that instance's id picks out non-default collations without needing
  // `private[sql]` helpers on `StringType`.
  def isStringCollationType(dt: DataType): Boolean = dt match {
    case st: StringType => st.collationId != StringType.collationId
    case _ => false
  }

  /**
   * Returns true if `dt`, or any nested element/field/key/value type, is a `StringType` with a
   * non-default (non-UTF8_BINARY) collation. Expression serdes can use this to fall back to Spark
   * when they cannot honour collation semantics. Stubbed to `false` in Spark 3.x.
   */
  def hasNonDefaultStringCollation(dt: DataType): Boolean = dt match {
    case _: StringType => isStringCollationType(dt)
    case ArrayType(elementType, _) => hasNonDefaultStringCollation(elementType)
    case MapType(kt, vt, _) =>
      hasNonDefaultStringCollation(kt) || hasNonDefaultStringCollation(vt)
    case StructType(fields) => fields.exists(f => hasNonDefaultStringCollation(f.dataType))
    case _ => false
  }

  // Spark 4.0's `PushVariantIntoScan` rewrites `VariantType` columns into a `StructType` whose
  // fields each carry `__VARIANT_METADATA_KEY` metadata, then pushes `variant_get` paths down as
  // ordinary struct field accesses. The direct whole-value scan path does not support that pushed
  // VariantStruct representation. Detect the marker and force scan fallback.
  def isVariantStruct(s: StructType): Boolean = VariantMetadata.isVariantStruct(s)

  // Outside direct top-level Parquet projection, Comet has no native execution path for Spark 4's
  // `VariantType` (introduced in SPARK-45827). Serdes call this to route casts and expressions
  // touching the type back to Spark. Stubbed to `false` in Spark 3.x.
  def isVariantType(dt: DataType): Boolean = dt.isInstanceOf[VariantType]

  def containsVariantType(dt: DataType): Boolean = dt match {
    case dt if isVariantType(dt) => true
    case StructType(fields) => fields.exists(field => containsVariantType(field.dataType))
    case ArrayType(elementType, _) => containsVariantType(elementType)
    case MapType(keyType, valueType, _) =>
      containsVariantType(keyType) || containsVariantType(valueType)
    case _ => false
  }

  def variantType: Option[DataType] = Some(VariantType)

  // Expose Variant defaults to the native scan as their Arrow storage struct without enabling
  // Variant literals in Comet's general expression serde.
  def variantDefaultExpression(value: Any): Option[Expression] = value match {
    case variant: VariantVal =>
      val variantValue = variant.getValue
      val metadata = variant.getMetadata
      if (variantValue == null || metadata == null) {
        None
      } else {
        Some(
          CreateNamedStruct(
            Seq(Literal("value"), Literal(variantValue), Literal("metadata"), Literal(metadata))))
      }
    case _ => None
  }

  def isTimeType(dt: DataType): Boolean =
    dt.getClass.getSimpleName.startsWith("TimeType")

  def hasCollationSupport: Boolean = true
}

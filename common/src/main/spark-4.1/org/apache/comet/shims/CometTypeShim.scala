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

import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.types.{DataType, StringType, StructType}

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

  // Spark 4.0's `PushVariantIntoScan` rewrites `VariantType` columns into a `StructType` whose
  // fields each carry `__VARIANT_METADATA_KEY` metadata, then pushes `variant_get` paths down as
  // ordinary struct field accesses. Comet's native scans don't understand the on-disk Parquet
  // variant shredding layout, so reading such a struct natively returns nulls. Detect the marker
  // and force scan fallback.
  def isVariantStruct(s: StructType): Boolean = VariantMetadata.isVariantStruct(s)
}

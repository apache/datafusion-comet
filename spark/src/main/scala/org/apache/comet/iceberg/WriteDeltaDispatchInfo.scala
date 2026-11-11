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

package org.apache.comet.iceberg

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{RowDeltaUtils, WriteDeltaProjections}
import org.apache.spark.sql.types.IntegerType

case class DeltaOperationCodes(delete: Int, update: Int, insert: Int, reinsert: Option[Int])

/**
 * Version-neutral task contract for Spark's operation-coded WriteDelta row stream.
 *
 * This intentionally contains only state consumed by Iceberg's JVM DeltaWriter path. Native
 * position-delete schemas and metadata layouts belong to the native WriteDelta implementation.
 */
case class WriteDeltaDispatchInfo(
    operationOrdinal: Int,
    operationCodes: DeltaOperationCodes,
    rowProjection: Option[ProjectingInternalRow],
    rowIdProjection: ProjectingInternalRow,
    metadataProjection: Option[ProjectingInternalRow],
    command: Option[DeltaCommand] = None)

private[iceberg] object WriteDeltaDispatchInfo {

  def build(
      projections: WriteDeltaProjections,
      childOutput: Seq[Attribute],
      operationCodes: DeltaOperationCodes,
      command: Option[DeltaCommand] = None): Option[WriteDeltaDispatchInfo] = {
    childOutput.headOption.collect {
      case attribute
          if attribute.name == RowDeltaUtils.OPERATION_COLUMN &&
            attribute.dataType == IntegerType &&
            !attribute.nullable =>
        WriteDeltaDispatchInfo(
          operationOrdinal = 0,
          operationCodes = operationCodes,
          rowProjection = projections.rowProjection,
          rowIdProjection = projections.rowIdProjection,
          metadataProjection = projections.metadataProjection,
          command = command)
    }
  }
}

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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.{DELETE_OPERATION, INSERT_OPERATION, REINSERT_OPERATION, UPDATE_OPERATION}
import org.apache.spark.sql.connector.write.DeltaWriter

/** Spark 4.x adds REINSERT to the WriteDelta operation stream. */
object IcebergDeltaWriterShim {
  val OperationCodes: DeltaOperationCodes =
    DeltaOperationCodes(
      delete = DELETE_OPERATION,
      update = UPDATE_OPERATION,
      insert = INSERT_OPERATION,
      reinsert = Some(REINSERT_OPERATION))

  def writeOperation(
      writer: DeltaWriter[InternalRow],
      operation: Int,
      row: InternalRow,
      info: WriteDeltaDispatchInfo): Unit = {
    def metadata(): InternalRow = info.metadataProjection.map { projection =>
      projection.project(row)
      projection
    }.orNull

    operation match {
      case code if code == info.operationCodes.delete =>
        info.rowIdProjection.project(row)
        writer.delete(metadata(), info.rowIdProjection)
      case code if code == info.operationCodes.update =>
        val data = info.rowProjection.getOrElse(
          throw new IllegalArgumentException("UPDATE operation has no row projection"))
        data.project(row)
        info.rowIdProjection.project(row)
        writer.update(metadata(), info.rowIdProjection, data)
      case code if code == info.operationCodes.insert =>
        val data = info.rowProjection.getOrElse(
          throw new IllegalArgumentException("INSERT operation has no row projection"))
        data.project(row)
        writer.insert(data)
      case code if info.operationCodes.reinsert.contains(code) =>
        val data = info.rowProjection.getOrElse(
          throw new IllegalArgumentException("REINSERT operation has no row projection"))
        data.project(row)
        writer.reinsert(metadata(), data)
      case _ =>
        throw new SparkException(s"Unexpected operation ID: $operation")
    }
  }
}

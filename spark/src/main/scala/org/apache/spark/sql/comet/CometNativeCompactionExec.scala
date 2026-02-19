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

package org.apache.spark.sql.comet

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Executes Iceberg compaction via Comet's native Rust/DataFusion engine. Replaces CallExec for
 * RewriteDataFilesProcedure when native compaction is enabled.
 *
 * Output row is built dynamically to match the procedure's output schema, which varies across
 * Iceberg versions (e.g. removed_delete_files_count added in later versions).
 */
case class CometNativeCompactionExec(
    output: Seq[Attribute],
    @transient tableCatalog: TableCatalog,
    tableIdent: Identifier)
    extends LeafV2CommandExec
    with Logging {

  override protected def run(): Seq[InternalRow] = {
    val spark = SparkSession.active
    val icebergTable = tableCatalog
      .loadTable(tableIdent)
      .asInstanceOf[SparkTable]
      .table()

    logInfo(s"Executing native compaction for $tableIdent")
    val summary = CometNativeCompaction(spark).rewriteDataFiles(icebergTable)

    val fieldValues: Map[String, Any] = Map(
      "rewritten_data_files_count" -> summary.filesDeleted,
      "added_data_files_count" -> summary.filesAdded,
      "rewritten_bytes_count" -> summary.bytesDeleted,
      "failed_data_files_count" -> 0,
      "removed_delete_files_count" -> 0)

    val values = output.map(attr => fieldValues.getOrElse(attr.name, 0))
    Seq(new GenericInternalRow(values.toArray))
  }

  override def simpleString(maxFields: Int): String =
    s"CometNativeCompactionExec[$tableIdent]"
}

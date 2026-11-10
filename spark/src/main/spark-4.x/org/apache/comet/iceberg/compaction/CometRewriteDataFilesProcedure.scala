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

package org.apache.comet.iceberg.compaction

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.catalog.procedures.{BoundProcedure, ProcedureParameter}
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import org.apache.comet.iceberg.IcebergRefreshCacheShim

/**
 * Iceberg's `rewrite_data_files` with Comet's rewrite in front of it.
 *
 * Every call is first checked against what [[CometRewriteDataFiles]] reproduces. A call that is
 * not covered, such as the `sort` or `zorder` strategies, an option Comet does not know, a
 * `where` that does not convert, or a table Comet cannot load, runs `delegate` instead. The check
 * changes nothing, so the fallback is safe. Once the rewrite has started, a failure is the call's
 * failure, the same as with Iceberg.
 *
 * @param delegate
 *   Iceberg's procedure. Its parameters, name and description are this procedure's.
 */
class CometRewriteDataFilesProcedure(
    session: SparkSession,
    catalog: TableCatalog,
    delegate: BoundProcedure)
    extends BoundProcedure
    with Logging {

  import CometRewriteDataFilesProcedure._

  override def name(): String = delegate.name()

  override def description(): String = delegate.description()

  override def parameters(): Array[ProcedureParameter] = delegate.parameters()

  override def isDeterministic: Boolean = delegate.isDeterministic

  override def call(input: InternalRow): java.util.Iterator[Scan] = {
    val prepared =
      try {
        prepare(input)
      } catch {
        case NonFatal(e) =>
          logWarning(
            "Comet cannot plan rewrite_data_files, so Iceberg's procedure runs it: " +
              e.getMessage,
            e)
          None
      }
    prepared match {
      case Some(rewrite) =>
        java.util.Collections.singleton[Scan](new Output(toRow(rewrite.execute()))).iterator()
      case None =>
        delegate.call(input)
    }
  }

  /** The planned rewrite, or None when the call is left to Iceberg. Changes nothing. */
  private def prepare(input: InternalRow): Option[CometRewriteDataFiles.PreparedRewrite] = {
    val params = parameters()
    def ordinal(name: String): Int = params.indexWhere(_.name() == name)
    def string(name: String): Option[String] = {
      val i = ordinal(name)
      if (i < 0 || input.isNullAt(i)) None else Some(input.getUTF8String(i).toString)
    }

    if (!outputMatches(delegate)) {
      return decline("the procedure's result schema is not the one Comet produces")
    }
    val strategy = string("strategy")
    if (string("sort_order").isDefined || strategy.exists(!_.equalsIgnoreCase("binpack"))) {
      return decline("only the binpack strategy without a sort order is supported")
    }

    val tableArgument = string("table").filter(_.nonEmpty)
    val identifier = tableArgument.flatMap { name =>
      IcebergRewriteReflection.resolveIdentifier(session, catalog, name)
    }
    if (identifier.isEmpty) {
      return decline("the table is not in the procedure's catalog")
    }
    val ident = identifier.get.asInstanceOf[Identifier]

    val sparkTable = catalog.loadTable(ident)
    if (!IcebergRewriteReflection.isSparkTable(sparkTable)) {
      return decline("the table is not an Iceberg table")
    }
    val icebergTable = IcebergRewriteReflection.icebergTable(sparkTable)

    val filter = string("where").map { where =>
      IcebergRewriteReflection.whereExpression(session, catalog.name(), ident, where)
    }
    val options = {
      val i = ordinal("options")
      if (i < 0 || input.isNullAt(i)) {
        Map.empty[String, String]
      } else {
        val map = input.getMap(i)
        val keys = map.keyArray()
        val values = map.valueArray()
        (0 until map.numElements())
          .map(n => keys.getUTF8String(n).toString -> values.getUTF8String(n).toString)
          .toMap
      }
    }

    val refresh = () =>
      IcebergRefreshCacheShim.refreshCache(
        session,
        DataSourceV2Relation.create(sparkTable, Some(catalog), Some(ident)))
    val request = CometRewriteDataFiles.Request(session, icebergTable, options, filter, refresh)
    CometRewriteDataFiles.prepare(request) match {
      case Right(rewrite) => Some(rewrite)
      case Left(reason) => decline(reason)
    }
  }

  private def decline[T](reason: String): Option[T] = {
    logInfo(s"Comet leaves this rewrite_data_files call to Iceberg: $reason")
    None
  }
}

object CometRewriteDataFilesProcedure {

  /** The result row Iceberg's procedure returns, which this one must match exactly. */
  private val OutputType = StructType(
    Seq(
      StructField("rewritten_data_files_count", IntegerType, nullable = false),
      StructField("added_data_files_count", IntegerType, nullable = false),
      StructField("rewritten_bytes_count", LongType, nullable = false),
      StructField("failed_data_files_count", IntegerType, nullable = false),
      StructField("removed_delete_files_count", IntegerType, nullable = false)))

  /**
   * Iceberg keeps the result schema in a private constant. A release that adds a column would
   * make Comet's row the wrong shape, so a mismatch sends the call to Iceberg.
   */
  private def outputMatches(delegate: BoundProcedure): Boolean =
    try {
      val field = delegate.getClass.getDeclaredField("OUTPUT_TYPE")
      field.setAccessible(true)
      field.get(null) == OutputType
    } catch {
      case NonFatal(_) => false
    }

  private def toRow(result: CometRewriteDataFiles.Result): Array[InternalRow] =
    Array(
      new GenericInternalRow(
        Array[Any](
          result.rewrittenDataFilesCount,
          result.addedDataFilesCount,
          result.rewrittenBytesCount,
          result.failedDataFilesCount,
          result.removedDeleteFilesCount)))

  private final class Output(data: Array[InternalRow]) extends LocalScan {
    override def readSchema(): StructType = OutputType

    override def rows(): Array[InternalRow] = data
  }
}

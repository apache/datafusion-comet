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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedProcedure
import org.apache.spark.sql.catalyst.plans.logical.{Call, LogicalPlan}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded

/**
 * Spark 4 runs a `CALL` while it analyzes the statement. `InvokeProcedures` optimizes the `Call`
 * and then invokes the procedure it holds, so the procedure object is the one place to intercept.
 * This swaps Iceberg's `rewrite_data_files` for [[CometRewriteDataFilesProcedure]], which runs
 * Comet's rewrite when it can and Iceberg's procedure otherwise.
 */
object CometProcedureShim {

  private val IcebergRewriteDataFiles =
    "org.apache.iceberg.spark.procedures.RewriteDataFilesProcedure"

  def wrap(session: SparkSession, plan: LogicalPlan): LogicalPlan = plan match {
    case call @ Call(
          resolved @ ResolvedProcedure(catalog: TableCatalog, ident, procedure),
          _,
          true)
        if isIcebergRewriteDataFiles(ident.namespace, ident.name, procedure) &&
          enabled(session) =>
      call.copy(procedure = resolved.copy(procedure = new CometRewriteDataFilesProcedure(
        session,
        catalog,
        procedure.asInstanceOf[BoundProcedure])))
    case _ => plan
  }

  private def isIcebergRewriteDataFiles(
      namespace: Array[String],
      name: String,
      procedure: AnyRef): Boolean =
    name == "rewrite_data_files" &&
      namespace.sameElements(Array("system")) &&
      procedure.isInstanceOf[BoundProcedure] &&
      procedure.getClass.getName == IcebergRewriteDataFiles

  private def enabled(session: SparkSession): Boolean = {
    val conf = session.sessionState.conf
    isCometLoaded(conf) && CometConf.COMET_ICEBERG_COMPACTION_ENABLED.get(conf)
  }
}

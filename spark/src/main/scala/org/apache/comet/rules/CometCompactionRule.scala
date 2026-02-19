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

package org.apache.comet.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.{CometNativeCompaction, CometNativeCompactionExec}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.CometConf

/**
 * Replaces Iceberg's CallExec targeting RewriteDataFilesProcedure with native Comet compaction.
 *
 * Uses reflection to detect CallExec (from Iceberg extensions) to avoid hard compile-time
 * dependency. Only active when spark.comet.iceberg.compaction.enabled is true and native
 * compaction is available. Currently supports Spark 3.x only (Spark 4.0 uses InvokeProcedures at
 * the analysis phase, handled separately via shim).
 */
case class CometCompactionRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  private val CALL_EXEC_CLASS = "org.apache.spark.sql.execution.datasources.v2.CallExec"
  private val REWRITE_PROCEDURE_NAME = "RewriteDataFilesProcedure"

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!isEnabled) return plan

    plan.transformUp {
      case exec if isRewriteCallExec(exec) =>
        replaceWithNative(exec).getOrElse(exec)
    }
  }

  private def isEnabled: Boolean =
    CometConf.COMET_ICEBERG_COMPACTION_ENABLED.get(session.sessionState.conf) &&
      CometNativeCompaction.isAvailable

  private def isRewriteCallExec(plan: SparkPlan): Boolean = {
    plan.getClass.getName == CALL_EXEC_CLASS && {
      try {
        val proc = plan.getClass.getMethod("procedure").invoke(plan)
        proc.getClass.getSimpleName == REWRITE_PROCEDURE_NAME
      } catch { case _: Exception => false }
    }
  }

  private def replaceWithNative(exec: SparkPlan): Option[SparkPlan] = {
    try {
      val proc = exec.getClass.getMethod("procedure").invoke(exec)
      val input = exec.getClass.getMethod("input").invoke(exec).asInstanceOf[InternalRow]

      // Only intercept bin-pack strategy (default when strategy is null)
      if (!input.isNullAt(1)) {
        val strategy = input.getUTF8String(1).toString
        if (!strategy.equalsIgnoreCase("binpack")) {
          logInfo(s"Native compaction skipped: unsupported strategy '$strategy'")
          return None
        }
      }

      val tableCatalog = extractTableCatalog(proc)
      val tableIdent = parseIdentifier(input.getUTF8String(0).toString)

      logInfo(s"Replacing CallExec with CometNativeCompactionExec for $tableIdent")
      Some(CometNativeCompactionExec(exec.output, tableCatalog, tableIdent))
    } catch {
      case e: Exception =>
        logWarning(s"Cannot replace with native compaction: ${e.getMessage}")
        None
    }
  }

  /** Extract TableCatalog from BaseProcedure via reflection (field is private). */
  private def extractTableCatalog(procedure: Any): TableCatalog = {
    val field = procedure.getClass.getSuperclass.getDeclaredField("tableCatalog")
    field.setAccessible(true)
    field.get(procedure).asInstanceOf[TableCatalog]
  }

  private def parseIdentifier(identStr: String): Identifier = {
    val parts = identStr.split("\\.")
    Identifier.of(parts.dropRight(1), parts.last)
  }
}

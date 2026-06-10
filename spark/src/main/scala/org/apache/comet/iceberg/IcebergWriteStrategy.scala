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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergWriteExec}
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import org.apache.comet.CometConf

/**
 * Spark Strategy that intercepts Iceberg V2 copy-on-write logical writes and emits Comet's
 * two-operator physical tree.
 */
case class IcebergWriteStrategy(session: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.get(session.sessionState.conf)) {
      return Nil
    }

    plan match {
      case ad: AppendData =>
        matchedSparkWrite(ad.table, ad.write, ad.query).toList
      case obe: OverwriteByExpression =>
        matchedSparkWrite(obe.table, obe.write, obe.query).toList
      case opd: OverwritePartitionsDynamic =>
        matchedSparkWrite(opd.table, opd.write, opd.query).toList
      // Hit by AQE.
      case IcebergWriteLogical(child, batchWrite) =>
        Seq(IcebergWriteExec(batchWrite, planLater(child)))
      case _ => Nil
    }
  }

  private def matchedSparkWrite(
      table: org.apache.spark.sql.catalyst.analysis.NamedRelation,
      write: Option[Write],
      query: LogicalPlan): Option[SparkPlan] = {
    table match {
      case rel: DataSourceV2Relation =>
        write.flatMap { w =>
          if (IcebergReflection.isIcebergSparkWrite(w)) {
            buildTwoOp(w, rel, query)
          } else {
            None
          }
        }
      case _ => None
    }
  }

  /**
   * Builds the two-op tree. The committer and writer share one `BatchWrite` (also reused across
   * AQE re-plans): `toBatch()` returns a fresh instance per call, but the committer's commit-time
   * validation must see the same instance the writer wrote through, hence we store it. The
   * writer's child is wrapped in [[IcebergWriteLogical]] so AQE re-emits only the data-writing
   * operator on each re-plan as opposed to multiple new commit operators.
   *
   * Returns None, falling back to Spark's combined write operator, when the `BatchWrite` requires
   * Spark's commit coordinator, which the split writer's per-task commit protocol does not use.
   */
  private def buildTwoOp(
      write: Write,
      rel: DataSourceV2Relation,
      query: LogicalPlan): Option[SparkPlan] = {
    val batchWrite = write.toBatch
    if (batchWrite.useCommitCoordinator()) {
      return None
    }
    val refresh: () => Unit = () => IcebergRefreshCacheShim.recacheByPlan(rel)
    Some(
      IcebergCommitExec(batchWrite, refresh, planLater(IcebergWriteLogical(query, batchWrite))))
  }
}

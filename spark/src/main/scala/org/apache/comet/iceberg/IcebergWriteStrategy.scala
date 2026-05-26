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
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceData}
import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergWriteExec}
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import org.apache.comet.CometConf

/**
 * Spark Strategy that intercepts Iceberg V2 copy-on-write logical writes and emits Comet's
 * two-operator physical tree (`IcebergCommitExec` committer + `IcebergWriteExec` writer) instead
 * of the default `V2ExistingTableWriteExec` family.
 *
 * Registered via `SparkSessionExtensions.injectPlannerStrategy` so it runs in
 * `experimentalMethods.extraStrategies`, ahead of Spark's built-in `DataSourceV2Strategy` (the
 * strategy list is concatenated in that order in `SparkPlanner.strategies`). Returning a
 * non-empty `Seq[SparkPlan]` short-circuits later strategies for the matched logical node.
 *
 * Guarded by [[CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED]] (off by default) and by
 * [[IcebergReflection.isIcebergSparkWrite]] -- non-Iceberg V2 writes fall through to Spark.
 *
 * Covers `AppendData` (INSERT INTO), `OverwriteByExpression` (INSERT OVERWRITE with a static
 * predicate), `OverwritePartitionsDynamic` (INSERT OVERWRITE with dynamic partition overwrite
 * mode), and `ReplaceData` (copy-on-write DML). `WriteDelta` (merge-on-read DML) is intentionally
 * left for Spark's default path: the per-task `DeltaWriter` is row-dispatched and the native
 * writer cannot emit position-delete files, so the split-operator plan would only add planning
 * complexity for no realisable acceleration.
 */
case class IcebergWriteStrategy(session: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.get(session.sessionState.conf)) {
      return Nil
    }

    plan match {
      case ad: AppendData =>
        matchedSparkWrite(ad.table, ad.write, ad.query, replaceDataDispatch = None).toList
      case obe: OverwriteByExpression =>
        matchedSparkWrite(obe.table, obe.write, obe.query, replaceDataDispatch = None).toList
      case opd: OverwritePartitionsDynamic =>
        matchedSparkWrite(opd.table, opd.write, opd.query, replaceDataDispatch = None).toList
      case rd: ReplaceData =>
        // CoW DML: refresh cache for the *original* table relation, mirroring Spark's
        // `DataSourceV2Strategy` case (which uses the second `DataSourceV2Relation` argument). The
        // `write` carries a `SparkWrite$CopyOnWriteOperation` -- still recognised by
        // `isIcebergSparkWrite`. The per-version shim extracts the 4.x `ReplaceDataProjections`
        // (absent on 3.4 / 3.5, returning None) so the writer can dispatch the
        // `__row_operation`-prefixed row stream.
        matchedSparkWrite(
          rd.originalTable,
          rd.write,
          rd.query,
          replaceDataDispatch = IcebergReplaceDataShim.extractProjections(rd)).toList
      // Iceberg 1.5.2 (Spark 3.4 + iceberg-spark-extensions) ships its own `ReplaceIcebergData`
      // logical write node because Spark 3.4 lacks native row-level operation support for
      // UPDATE / MERGE on V2 tables. Field shape matches Spark 3.5's stock node one-for-one;
      // matched by FQCN so the main module stays free of an iceberg-spark-extensions compile
      // dep. On 3.5+ / Iceberg 1.8+ the class doesn't exist, the guard returns false, and the
      // case is a no-op.
      case plan if IcebergReflection.isReplaceIcebergData(plan) =>
        IcebergReflection
          .extractReplaceIcebergDataFields(plan)
          .flatMap { case (_, query, originalTable, write) =>
            matchedSparkWrite(
              originalTable.asInstanceOf[org.apache.spark.sql.catalyst.analysis.NamedRelation],
              write.asInstanceOf[Option[Write]],
              query.asInstanceOf[LogicalPlan],
              replaceDataDispatch = None)
          }
          .toList
      // Hit by AQE.
      case IcebergWriteLogical(child, batchWrite, replaceDataDispatch) =>
        Seq(IcebergWriteExec(batchWrite, planLater(child), replaceDataDispatch))
      case _ => Nil
    }
  }

  /**
   * Common path for the four `Write`-backed logical write nodes. Guarded on
   * `IcebergReflection.isIcebergSparkWrite` so non-Iceberg DSv2 writes fall through. The
   * `replaceDataDispatch` is `Some` only for Spark 4.x `ReplaceData` (extracted via shim), and
   * controls the per-row dispatch in the writer.
   */
  private def matchedSparkWrite(
      table: org.apache.spark.sql.catalyst.analysis.NamedRelation,
      write: Option[Write],
      query: LogicalPlan,
      replaceDataDispatch: Option[ReplaceDataDispatchInfo]): Option[SparkPlan] = {
    table match {
      case rel: DataSourceV2Relation =>
        write.flatMap { w =>
          if (IcebergReflection.isIcebergSparkWrite(w)) {
            Some(buildTwoOp(w, rel, query, replaceDataDispatch))
          } else {
            None
          }
        }
      case _ => None
    }
  }

  /**
   * Construct the two-op tree.
   *
   *   - The `BatchWrite` is materialised once and shared between the committer and the writer.
   *     Iceberg's `Write.toBatch()` mints a fresh `BatchWrite` on every call, and for
   *     copy-on-write DML the BatchWrite carries the scan state and emitted-file tracking that
   *     the committer's validation needs to see -- so both execs must run against the same
   *     instance.
   *
   *   - The writer's child plan is wrapped in `IcebergWriteLogical` to give AQE a stable logical
   *     anchor each time a stage materialises. Every physical node in Spark carries a
   *     `logicalLink` pointing back at the logical-plan node it was produced from. AQE uses those
   *     links as the unit of re-optimisation: when a stage materialises and new runtime stats
   *     arrive, AQE picks up `logicalLink`, re-runs the optimizer + the SparkPlanner against that
   *     logical subtree, and substitutes the result back into the physical tree. What that
   *     subtree contains is therefore what gets re-emitted on every iteration. We aim the
   *     writer's link at `IcebergWriteLogical` so each re-plan re-emits just the writer (via the
   *     second `case IcebergWriteLogical(...)` clause in `apply`). This keeps the commit singular
   *     and lets the data sub-query underneath be optimised by AQE as expected.
   *
   *   - `refreshCache` is what Spark normally runs after a V2 write to invalidate any cached
   *     `DataFrame` / `Dataset` views of this table so subsequent reads see the new snapshot.
   *     Since we replace Spark's V2 write exec we have to invoke it ourselves; the committer runs
   *     it after the snapshot commits. The shim hides Spark 3.x vs 4.x's diverging `SparkSession`
   *     shape.
   */
  private def buildTwoOp(
      write: Write,
      rel: DataSourceV2Relation,
      query: LogicalPlan,
      replaceDataDispatch: Option[ReplaceDataDispatchInfo]): SparkPlan = {
    val batchWrite = write.toBatch
    val refresh: () => Unit = () => IcebergRefreshCacheShim.recacheByPlan(rel)
    IcebergCommitExec(
      batchWrite,
      refresh,
      planLater(IcebergWriteLogical(query, batchWrite, replaceDataDispatch)))
  }
}

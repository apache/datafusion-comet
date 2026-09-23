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

package org.apache.comet.parquet

import java.util.concurrent.atomic.AtomicReference

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometNativeWriteExec, CometWriteFilesExec}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

abstract class CometParquetWriterTestBase extends CometTestBase {

  protected def withNativeWriter(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      nativeWriteAllowIncompatKey -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax")(f)
  }

  /** The opt-in config key for native writes, which moved with the operator on Spark 4.0+. */
  protected def nativeWriteAllowIncompatKey: String =
    if (isSpark40Plus) {
      CometConf.COMET_OPERATOR_WRITE_FILES_ALLOW_INCOMPAT.key
    } else {
      CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key
    }

  /**
   * Captures the execution plan during a write operation.
   *
   * @param writeOp
   *   The write operation to execute (takes output path as parameter)
   * @param outputPath
   *   The path to write to
   * @return
   *   The captured execution plan
   */
  protected def captureWritePlan(writeOp: String => Unit, outputPath: String): SparkPlan =
    captureWritePlan(new Path(outputPath))(writeOp(outputPath))

  /**
   * Captures the execution plan of the write to `expectedOutputPath`.
   *
   * Only an [[InsertIntoHadoopFsRelationCommand]] whose output path is that target is accepted.
   * The first `save` or command event is not enough: a preceding `INSERT VALUES` can arrive after
   * this listener is registered, and that event belongs to a different write.
   *
   * @param expectedOutputPath
   *   output path of the write to capture
   * @param writeOp
   *   the write to execute
   * @return
   *   the captured execution plan
   */
  protected def captureWritePlan(expectedOutputPath: Path)(writeOp: => Unit): SparkPlan = {
    val capturedPlan = new AtomicReference[QueryExecution]()

    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        if (writesTo(qe, expectedOutputPath)) {
          capturedPlan.set(qe)
        }
      }

      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)

    try {
      writeOp

      // The matching callback can still be queued after the write returns.
      val maxWaitTimeMs = 15000
      val checkIntervalMs = 100
      val maxIterations = maxWaitTimeMs / checkIntervalMs
      var iterations = 0

      while (capturedPlan.get() == null && iterations < maxIterations) {
        Thread.sleep(checkIntervalMs)
        iterations += 1
      }

      val captured = Option(capturedPlan.get())
      assert(
        captured.isDefined,
        s"No write to $expectedOutputPath was captured within ${maxWaitTimeMs}ms")

      stripAQEPlan(captured.get.executedPlan)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  /** Whether `qe` is an insert into `expectedOutputPath`. */
  private def writesTo(qe: QueryExecution, expectedOutputPath: Path): Boolean =
    insertOutputPath(qe).exists(actual => sameOutputPath(actual, expectedOutputPath))

  /**
   * Output path of the insert command, from the optimized logical plan. On Spark 3.x the physical
   * plan may already be a [[CometNativeWriteExec]], so the command is read from the logical plan
   * first and from [[DataWritingCommandExec]] only as a fallback.
   */
  private def insertOutputPath(qe: QueryExecution): Option[Path] = {
    val fromOptimized = qe.optimizedPlan.collectFirst {
      case cmd: InsertIntoHadoopFsRelationCommand => cmd.outputPath
    }
    fromOptimized.orElse {
      qe.executedPlan.collectFirst { case exec: DataWritingCommandExec =>
        exec.cmd
      } match {
        case Some(cmd: InsertIntoHadoopFsRelationCommand) => Some(cmd.outputPath)
        case _ => None
      }
    }
  }

  /**
   * Spark qualifies file-format write paths when building [[InsertIntoHadoopFsRelationCommand]],
   * while the helper may receive an unqualified temporary path. Re-parsing `Path.toString` does
   * not drop a `file:` scheme, so both sides are qualified through Hadoop `FileSystem` before
   * comparison.
   */
  private def sameOutputPath(actual: Path, expected: Path): Boolean = {
    val conf = spark.sessionState.newHadoopConf()
    val actualFs = actual.getFileSystem(conf)
    val expectedFs = expected.getFileSystem(conf)
    actualFs.makeQualified(actual) == expectedFs.makeQualified(expected)
  }

  /**
   * The operator that carries a native write, which differs by Spark version: on 4.0+ Comet
   * replaces only `WriteFilesExec` with [[CometWriteFilesExec]] and leaves Spark's write
   * framework in place, while on 3.x it replaces the whole `DataWritingCommandExec` with
   * [[CometNativeWriteExec]]. See `CometWriteFiles` / `CometDataWritingCommand`.
   */
  protected def isNativeWriteExec(plan: SparkPlan): Boolean = plan match {
    case _: CometWriteFilesExec => isSpark40Plus
    case _: CometNativeWriteExec => !isSpark40Plus
    case _ => false
  }

  protected def assertHasCometNativeWriteExec(plan: SparkPlan): Unit = {
    var nativeWriteCount = 0
    plan.foreach(p => if (isNativeWriteExec(p)) nativeWriteCount += 1)

    assert(
      nativeWriteCount == 1,
      "Expected exactly one native write operator in the plan, but found " +
        s"$nativeWriteCount:\n${plan.treeString}")

    if (isSpark40Plus) {
      // On 4.0+ the command is left in the plan on purpose for a fully native write, so it must
      // not be reported as a fallback - otherwise extended explain tells users an accelerated
      // write was not accelerated, and skews the "Comet accelerated N of M operators" count.
      plan.foreach {
        case d: DataWritingCommandExec =>
          val reasons = d.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
          assert(
            reasons.isEmpty,
            s"A fully native write must not tag ${d.nodeName} as a fallback, got: $reasons")
        case _ =>
      }
    }
  }

  protected def assertNoCometNativeWriteExec(plan: SparkPlan): Unit = {
    val hasNativeWrite = plan.exists(isNativeWriteExec)

    assert(
      !hasNativeWrite,
      s"Expected no native write operator in the plan, but found one:\n${plan.treeString}")
  }
}

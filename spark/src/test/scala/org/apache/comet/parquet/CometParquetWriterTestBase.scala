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

import java.io.File
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicReference

import scala.util.control.NonFatal

import org.apache.spark.CometListenerBusUtils
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.{CometNativeWriteExec, CometWriteFilesExec}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.hadoop.fs.RecordingFileSystem

abstract class CometParquetWriterTestBase extends CometTestBase {

  protected def withNativeWriter(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      nativeWriteAllowIncompatKey -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax")(f)
  }

  // Persist `df` to `sourcePath` with Comet disabled and return a DataFrame that reads it back,
  // so the source plan is a Comet scan (satisfying CometExecRule.requiresNativeChildren).
  protected def materializeAsCometSource(df: DataFrame, sourcePath: String): DataFrame = {
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      df.write.parquet(sourcePath)
    }
    spark.read.parquet(sourcePath)
  }

  /** The opt-in config key for native writes, which moved with the operator on Spark 4.0+. */
  /**
   * Runs `write` with a source of `rows` rows in `partitions` partitions, see the overload below.
   */
  protected def withRecordingFileSystem(rows: Int, partitions: Int)(
      write: (DataFrame, String) => Unit): Unit = {
    val session = spark
    import session.implicits._
    withRecordingFileSystem(
      (1 to rows).map(i => (i, s"str_$i")).toDF("id", "name").repartition(partitions))(write)
  }

  /**
   * Runs `write` with `source` read natively from a local copy, and the path to write it to,
   * while RecordingFileSystemCommitProtocol puts the write's task files on
   * [[RecordingFileSystem]], whose recordings start empty and are cleared afterwards.
   *
   * The output directory's name has a space, `%`, `#`, `?` and a non-ASCII character, which a
   * path has to keep as it passes from the commit protocol to Hadoop. The character is left out
   * when the JVM cannot encode it in a file name, as in a container with the POSIX locale, where
   * even the job's own cleanup of the directory fails.
   */
  protected def withRecordingFileSystem(source: => DataFrame)(
      write: (DataFrame, String) => Unit): Unit =
    withTempPath { dir =>
      val nonAscii = if (canEncodeInFileName('\u00e9')) "\u00e9" else ""
      val outputPath = new File(dir, s"output dir %#?$nonAscii.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      withNativeWriter {
        val df = materializeAsCometSource(source, sourcePath)
        RecordingFileSystem.reset()
        RecordingFileSystemCommitProtocol.reset()
        try {
          withSQLConf(
            SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
              classOf[RecordingFileSystemCommitProtocol].getName,
            s"fs.${RecordingFileSystem.SCHEME}.impl" -> classOf[RecordingFileSystem].getName) {
            write(df, outputPath)
          }
        } finally {
          RecordingFileSystem.reset()
          RecordingFileSystemCommitProtocol.reset()
        }
      }
    }

  /** Whether the JVM can name a file with `c`, which depends on `sun.jnu.encoding`. */
  private def canEncodeInFileName(c: Char): Boolean = {
    val encoding = System.getProperty("sun.jnu.encoding", "UTF-8")
    Charset.isSupported(encoding) && Charset.forName(encoding).newEncoder().canEncode(c)
  }

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
    captureWritePlan(writeOp(outputPath))

  /** As above, for a write that names its own target (an `INSERT INTO`, for example). */
  protected def captureWritePlan(writeOp: => Unit): SparkPlan = {
    val (plan, failure) = captureWriteOutcome(writeOp)
    failure.foreach(throw _)
    plan.getOrElse(fail("Listener was not called - no execution plan captured"))
  }

  /**
   * As above, for a write that is expected to fail. Returns the plan of the failed write, so a
   * test can check that the failure came from the native writer rather than from Spark's.
   */
  protected def captureFailedWritePlan(writeOp: => Unit): (SparkPlan, Throwable) = {
    val (plan, failure) = captureWriteOutcome(writeOp)
    val error = failure.getOrElse(fail("Expected the write to fail, but it succeeded"))
    (plan.getOrElse(fail("Listener was not called - no execution plan captured")), error)
  }

  private def captureWriteOutcome(writeOp: => Unit): (Option[SparkPlan], Option[Throwable]) = {
    val capturedPlan = new AtomicReference[QueryExecution]()

    def isWrite(funcName: String): Boolean = funcName == "save" || funcName.contains("command")
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        if (isWrite(funcName)) {
          capturedPlan.set(qe)
        }
      }

      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        if (isWrite(funcName)) {
          capturedPlan.set(qe)
        }
      }
    }

    // Listener events are delivered asynchronously, so drain the bus before registering: an
    // earlier write's event still in flight would otherwise be captured in place of this one.
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    spark.listenerManager.register(listener)

    try {
      val failure =
        try {
          writeOp
          None
        } catch {
          case NonFatal(e) => Some(e)
        }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      (Option(capturedPlan.get()).map(qe => stripAQEPlan(qe.executedPlan)), failure)
    } finally {
      spark.listenerManager.unregister(listener)
    }
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

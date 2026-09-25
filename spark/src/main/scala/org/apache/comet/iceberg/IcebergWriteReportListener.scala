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

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}
import java.util.UUID

import scala.util.control.NonFatal

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometIcebergWriteExec, IcebergWriteExec}
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.execution.{CommandResultExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.datasources.v2.{V2ExistingTableWriteExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf.COMET_ICEBERG_WRITE_REPORT_DIR
import org.apache.comet.CometExplainInfo

/**
 * Test-only listener that records which writer ran each Iceberg write, so a CI job running
 * Iceberg's own Spark suites can tell a native write from a silent fallback. The Comet driver
 * plugin registers it when `spark.comet.testing.icebergWriteReport.dir` is set. Each write is
 * appended as one JSON line to a file of its own in that directory, which
 * `dev/ci/summarize-iceberg-writes.py` reads.
 */
class IcebergWriteReportListener(conf: SparkConf) extends QueryExecutionListener with Logging {

  private val reportFile: File = {
    val dir = new File(
      conf
        .get(COMET_ICEBERG_WRITE_REPORT_DIR.key, COMET_ICEBERG_WRITE_REPORT_DIR.defaultValue.get))
    dir.mkdirs()
    // One file per listener: Gradle runs several test JVMs, each possibly with several sessions.
    new File(dir, s"iceberg-writes-${UUID.randomUUID()}.jsonl")
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    record(qe, failed = false)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    record(qe, failed = true)

  private def record(qe: QueryExecution, failed: Boolean): Unit = {
    try {
      val lines = IcebergWriteReportListener.writes(qe.executedPlan).map { w =>
        compact(
          render(("writer" -> w.writer) ~ ("node" -> w.node) ~ ("reasons" -> w.reasons.toList) ~
            ("failed" -> failed))) + "\n"
      }
      if (lines.nonEmpty) {
        synchronized {
          Files.write(
            reportFile.toPath,
            lines.mkString.getBytes(UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND)
        }
      }
    } catch {
      // A query that failed during planning has no executed plan; nothing was written.
      case NonFatal(e) => logWarning(s"Could not record Iceberg writes for a query: $e")
    }
  }
}

object IcebergWriteReportListener {

  /** Comet's native (iceberg-rust) writer ran the write. */
  val Native = "native"

  /** Comet's split operator planned the write, but Iceberg's JVM writer ran it. */
  val Jvm = "jvm"

  /** Spark's own V2 write operator ran the write; Comet's split operator did not plan it. */
  val Spark = "spark"

  case class IcebergWrite(writer: String, node: String, reasons: Seq[String])

  /** The Iceberg writes in an executed plan, with the reasons Comet did not write natively. */
  def writes(plan: SparkPlan): Seq[IcebergWrite] = plan match {
    // A command's writes are reported by the command's own execution, which runs eagerly before
    // the query wrapping its result. Descending here would count them twice.
    case _: CommandResultExec => Nil
    case a: AdaptiveSparkPlanExec => writes(a.executedPlan)
    case s: QueryStageExec => writes(s.plan)
    case w: CometIcebergWriteExec => Seq(IcebergWrite(Native, w.nodeName, Nil))
    case w: IcebergWriteExec =>
      val reasons = w.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
      Seq(IcebergWrite(Jvm, w.nodeName, reasons.toSeq.sorted))
    case w: V2ExistingTableWriteExec if isIceberg(w.write) =>
      Seq(IcebergWrite(Spark, w.nodeName, Nil))
    // A streaming micro-batch, which Comet's split operator never plans.
    case w: WriteToDataSourceV2Exec if isIcebergMicroBatch(w.batchWrite) =>
      Seq(IcebergWrite(Spark, w.nodeName, Nil))
    // Spark 3.4 writes a CTAS or RTAS from the create or replace exec itself. Later versions run
    // that write as a nested append or overwrite, which this listener sees as a query of its own.
    case w if IcebergTableAsSelectShim.writeCatalog(w).exists(isIceberg) =>
      Seq(IcebergWrite(Spark, w.nodeName, Nil))
    case p => p.children.flatMap(writes)
  }

  private def isIcebergMicroBatch(write: BatchWrite): Boolean = write match {
    case m: MicroBatchWrite => isIceberg(m.writeSupport)
    case _ => false
  }

  private def isIceberg(obj: AnyRef): Boolean =
    obj.getClass.getName.startsWith("org.apache.iceberg.")
}

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

import java.io.{File, IOException}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.internal.io.FileNameSpec
import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, SQLHadoopMapReduceCommitProtocol}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.{schema2Proto, CometDataWritingCommand}

/** Exercises the retained Spark 3.x writer, including both terminal execution entry points. */
class CometNativeWriteSuite extends CometTestBase {
  import NativeWriteCommitProtocol._

  private def withWriter(partitions: Int = 1)(
      f: (CometNativeWriteExec, DataFrame, File) => Unit): Unit = {
    assume(!isSpark40Plus, "Spark 4.0+ uses CometWriteFilesExec")
    withTempPath { dir =>
      val source = new File(dir, "source").getAbsolutePath
      val output = new File(dir, "output with % space")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.range(0, 20, 1, partitions).write.parquet(source)
      }
      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key -> classOf[NativeWriteCommitProtocol].getName,
        CometConf.COMET_METRICS_UPDATE_INTERVAL.key -> "0") {
        val data = spark.read.parquet(source)
        val sourcePlan = data.queryExecution.executedPlan
        val child = sourcePlan
          .collectFirst { case scan: CometNativeScanExec =>
            scan
          }
          .getOrElse(fail(s"Expected a native source scan: $sourcePlan"))
        val cmd = InsertIntoHadoopFsRelationCommand(
          outputPath = new Path(output.toURI),
          staticPartitions = Map.empty,
          ifPartitionNotExists = false,
          partitionColumns = Seq.empty,
          bucketSpec = None,
          fileFormat = new ParquetFileFormat,
          options = Map("compression" -> "gzip", "comet.test.option" -> "present"),
          query = data.queryExecution.analyzed,
          mode = SaveMode.ErrorIfExists,
          catalogTable = None,
          fileIndex = None,
          outputColumnNames = data.columns.toSeq)
        val command = DataWritingCommandExec(cmd, child)
        val nativeOp = CometDataWritingCommand.convert(command, Operator.newBuilder()).get
        val writer = CometDataWritingCommand
          .createExec(nativeOp, command)
          .asInstanceOf[CometNativeWriteExec]
        assert(writer.committer.isInstanceOf[NativeWriteCommitProtocol])
        reset()
        try f(writer, data, output)
        finally reset()
      }
    }
  }

  Seq(false, true).foreach { columnar =>
    test(
      s"configured commit protocol completes the ${if (columnar) "columnar" else "row"} write") {
      withWriter(2) { (writer, data, output) =>
        val resultCount =
          if (columnar) writer.executeColumnar().count() else writer.execute().count()
        assert(resultCount == 0)
        assert(events.asScala.head == "setupJob")
        assert(events.asScala.last == "commitJob")
        assert(!events.asScala.exists(_.startsWith("abort")))
        assert(writer.metrics("rows_written").value == 20, "the writer must execute natively")
        val files = allocated.asScala.map(p => new Path(p).getName).toSet
        assert(files.nonEmpty)
        assert(files.forall(n => n.startsWith("chosen % ") && n.endsWith(".gz.parquet")))
        assert(output.list().filter(_.endsWith(".parquet")).toSet == files)
        assert(new File(output, "_SUCCESS").isFile)
        assert(!new File(output, "_temporary").exists())
        checkAnswer(spark.read.parquet(output.getAbsolutePath), data)
      }
    }
  }

  test("native iterator creation failure aborts the task and job") {
    withWriter() { (writer, _, output) =>
      // Inject an unknown native config after the child scan has initialized, so the writer's
      // CometExecIterator constructor fails in createPlan, before the drain/close block is entered.
      failAt = "createIterator"
      failAbort = true
      val error = intercept[Exception](writer.execute().count())
      val causes = allErrors(error)
      assert(causes.exists(_.getStackTrace.exists(_.getMethodName == "createPlan")))
      assert(causes.exists(e => Option(e.getMessage).exists(_.contains("no_such_namespace"))))
      assert(causes.exists(_.getSuppressed.exists(_.getMessage == "injected abortTask failure")))
      assert(error.getSuppressed.exists(_.getMessage == "injected abortJob failure"))
      assertAborted(output)
      assert(!events.contains("commitTask"))
    }
  }

  test("native execution failure survives throwing cleanup and abort callbacks") {
    withWriter() { (writer, _, output) =>
      // A mismatched output type fails when writing the first native batch, after the native
      // stream is initialized. This also lets releasePlan exercise its final metrics callback.
      val badWriter = writer.nativeOp.getParquetWriter.toBuilder
        .clearOutputSchema()
        .addAllOutputSchema(schema2Proto(Seq(StructField("id", StringType))).asJava)
        .build()
      val badPlan = writer.nativeOp.toBuilder.setParquetWriter(badWriter).build()
      failAbort = true
      val failingWriter = new CleanupFailingNativeWriteExec(writer.copy(nativeOp = badPlan))
      val error = intercept[Exception](failingWriter.executeColumnar().count())
      val causes = allErrors(error)
      assert(causes.exists(_.getStackTrace.exists(_.getMethodName == "executePlan")))
      assert(causes.exists(e =>
        Option(e.getMessage).exists(_.contains("Failed to rename batch columns"))))
      assert(causes.exists(_.getSuppressed.exists(_.getMessage == "injected cleanup failure")))
      assert(causes.exists(_.getSuppressed.exists(_.getMessage == "injected abortTask failure")))
      assert(error.getSuppressed.exists(_.getMessage == "injected abortJob failure"))
      assertAborted(output)
      assert(!events.contains("commitTask"))
    }
  }

  test("a cleanup failure aborts instead of committing") {
    withWriter() { (writer, _, output) =>
      val error =
        intercept[Exception](new CleanupFailingNativeWriteExec(writer).execute().count())
      assert(allErrors(error).exists(_.getMessage == "injected cleanup failure"))
      assertAborted(output)
      assert(!events.contains("commitTask"))
    }
  }

  Seq("commitTask", "onTaskCommit", "commitJob").foreach { phase =>
    test(s"$phase failure aborts and preserves the commit failure") {
      withWriter() { (writer, _, output) =>
        failAt = phase
        failAbort = true
        val error = intercept[Exception](writer.execute().count())
        assert(allErrors(error).exists(_.getMessage == s"injected $phase failure"))
        assert(error.getSuppressed.exists(_.getMessage == "injected abortJob failure"))
        assert(events.contains("abortJob"))
        if (phase == "commitTask") {
          assert(events.contains("abortTask"))
          assert(
            allErrors(error).exists(
              _.getSuppressed.exists(_.getMessage == "injected abortTask failure")))
        }
        assert(!new File(output, "_SUCCESS").exists())
        assert(!new File(output, "_temporary").exists())
        assert(
          written.asScala.nonEmpty,
          "the failure must occur after the native file was written")
      }
    }
  }

  private def allErrors(error: Throwable): Seq[Throwable] =
    Seq(error) ++ Option(error.getCause).toSeq.flatMap(allErrors) ++
      error.getSuppressed.toSeq.flatMap(allErrors)

  private def assertAborted(output: File): Unit = {
    assert(events.contains("abortTask"))
    assert(events.contains("abortJob"))
    assert(!events.contains("commitJob"))
    assert(!new File(output, "_SUCCESS").exists())
    assert(!new File(output, "_temporary").exists())
    assert(allocated.asScala.nonEmpty, "the test must reach task file allocation")
  }
}

/**
 * Local-mode probes are reset for each write; executor and driver callbacks share this object.
 */
object NativeWriteCommitProtocol {
  val events = new ConcurrentLinkedQueue[String]()
  val allocated = new ConcurrentLinkedQueue[String]()
  val written = new ConcurrentLinkedQueue[String]()
  val received = new ConcurrentLinkedQueue[TaskCommitMessage]()
  @volatile var failAt: String = ""
  @volatile var failAbort = false

  def reset(): Unit = {
    events.clear()
    allocated.clear()
    written.clear()
    received.clear()
    failAt = ""
    failAbort = false
  }
}

class NativeWriteCommitProtocol(jobId: String, path: String, dynamicPartitionOverwrite: Boolean)
    extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {
  import NativeWriteCommitProtocol._
  private var taskFile: String = _

  override def setupJob(context: JobContext): Unit = {
    assert(context.getConfiguration.get("comet.test.option") == "present")
    assert(context.getOutputFormatClass == classOf[ParquetOutputFormat[_]])
    assert(FileOutputFormat.getOutputPath(context) == new Path(path))
    assert(context.getConfiguration.get("spark.sql.sources.writeJobUUID") != null)
    super.setupJob(context)
    context.getConfiguration.set("comet.test.setupJob", "present")
    events.add("setupJob")
  }

  override def setupTask(context: TaskAttemptContext): Unit = {
    assert(context.getConfiguration.get("comet.test.option") == "present")
    assert(context.getConfiguration.get("comet.test.setupJob") == "present")
    super.setupTask(context)
    events.add("setupTask")
  }

  override def newTaskTempFile(
      context: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String = {
    val sparkPath = new Path(super.newTaskTempFile(context, dir, spec))
    taskFile = new Path(sparkPath.getParent, "chosen % " + sparkPath.getName).toString
    allocated.add(taskFile)
    if (failAt == "createIterator") {
      val properties = TaskContext.get().getLocalProperties
      properties.setProperty(CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.key, "true")
      properties.setProperty("spark.comet.datafusion.no_such_namespace.option", "1")
    }
    taskFile
  }

  override def commitTask(context: TaskAttemptContext): TaskCommitMessage = {
    val file = new Path(taskFile)
    assert(file.getFileSystem(context.getConfiguration).getFileStatus(file).getLen > 0)
    written.add(taskFile)
    events.add("commitTask")
    if (failAt == "commitTask") throw new IOException("injected commitTask failure")
    new TaskCommitMessage((context.getTaskAttemptID.toString, super.commitTask(context)))
  }

  override def onTaskCommit(message: TaskCommitMessage): Unit = {
    events.add("onTaskCommit")
    received.add(message)
    if (failAt == "onTaskCommit") throw new IOException("injected onTaskCommit failure")
    super.onTaskCommit(message.obj.asInstanceOf[(String, TaskCommitMessage)]._2)
  }

  override def commitJob(context: JobContext, messages: Seq[TaskCommitMessage]): Unit = {
    assert(messages.nonEmpty)
    assert(messages.toSet == received.asScala.toSet)
    assert(
      messages.map(_.obj.asInstanceOf[(String, TaskCommitMessage)]._1).distinct.size ==
        messages.size)
    events.add("commitJob")
    if (failAt == "commitJob") throw new IOException("injected commitJob failure")
    super.commitJob(context, messages.map(_.obj.asInstanceOf[(String, TaskCommitMessage)]._2))
  }

  override def abortTask(context: TaskAttemptContext): Unit = {
    events.add("abortTask")
    super.abortTask(context)
    if (failAbort) throw new IOException("injected abortTask failure")
  }

  override def abortJob(context: JobContext): Unit = {
    events.add("abortJob")
    super.abortJob(context)
    if (failAbort) throw new IOException("injected abortJob failure")
  }
}

/** With periodic updates disabled, this metric throws only during native iterator teardown. */
class CleanupFailingWriteMetric extends SQLMetric("sum") {
  override def copy(): SQLMetric = new CleanupFailingWriteMetric
  override def set(value: Long): Unit = throw new IOException("injected cleanup failure")
}

class CleanupFailingNativeWriteExec(writer: CometNativeWriteExec)
    extends CometNativeWriteExec(
      writer.nativeOp,
      writer.child,
      writer.outputPath,
      writer.mode,
      writer.committer,
      writer.serializableHadoopConf,
      writer.outputWriterFactory,
      writer.jobTrackerID) {
  override lazy val metrics: Map[String, SQLMetric] = {
    val failingMetric = new CleanupFailingWriteMetric
    sparkContext.register(failingMetric)
    writer.metrics.updated("rows_written", failingMetric)
  }
}

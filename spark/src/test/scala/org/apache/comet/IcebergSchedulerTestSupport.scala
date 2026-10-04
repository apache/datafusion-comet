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

package org.apache.comet

import java.lang.management.ManagementFactory
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.json4s.{DefaultFormats, Extraction}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorRemoved, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.comet.IcebergSchedulerTestProbe

import org.apache.comet.serde.OperatorOuterClass.IcebergWriteTestProbe

private[comet] case class SchedulerAttempt(
    stage: Int,
    stageAttempt: Int,
    partition: Int,
    attempt: Long,
    number: Int,
    executor: String,
    host: String,
    pid: Long,
    files: Seq[String] = Nil,
    rows: Long = 0L)

private[comet] object IcebergSchedulerFiles {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def write(path: Path, value: AnyRef): Unit = {
    Files.createDirectories(path.getParent)
    val temporary = path.resolveSibling(path.getFileName.toString + ".pending")
    Files.write(temporary, compact(render(Extraction.decompose(value))).getBytes(UTF_8))
    Files.move(
      temporary,
      path,
      StandardCopyOption.ATOMIC_MOVE,
      StandardCopyOption.REPLACE_EXISTING)
    ()
  }

  def read(path: Path): SchedulerAttempt =
    parse(new String(Files.readAllBytes(path), UTF_8)).extract[SchedulerAttempt]

  def touch(path: Path): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, Array.emptyByteArray)
    ()
  }

  def paths(root: Path): Seq[Path] = {
    if (!Files.exists(root)) return Nil
    val stream = Files.walk(root)
    try stream.iterator().asScala.filter(Files.isRegularFile(_)).toVector
    finally stream.close()
  }

  def attempts(root: Path, event: String): Seq[SchedulerAttempt] =
    paths(root).filter(_.getFileName.toString == event + ".json").map(read)

  def progress(root: Path): Seq[SchedulerAttempt] = {
    paths(root).filter(_.getFileName.toString == "native-progress.json").map { path =>
      val data = parse(new String(Files.readAllBytes(path), UTF_8))
      read(path.getParent.resolve("start.json"))
        .copy(files = (data \ "files").extract[Seq[String]], rows = (data \ "rows").extract[Long])
    }
  }

  def await(description: String, timeoutMillis: Long)(condition: => Boolean)(
      diagnostics: => String): Unit = {
    val deadline = System.nanoTime() + timeoutMillis * 1000000L
    while (!condition) {
      if (System.nanoTime() >= deadline) {
        throw new AssertionError(s"Timed out waiting for $description\n$diagnostics")
      }
      // Polling a shared filesystem is bounded; event predicates decide when to continue.
      Thread.sleep(25L)
    }
  }

  def localPath(location: String): Path = {
    val uri = new URI(location)
    require(uri.getScheme == null || uri.getScheme == "file", s"POSIX path required: $location")
    (if (uri.getScheme == null) Paths.get(location) else Paths.get(uri)).toAbsolutePath
      .normalize()
  }

  def relative(root: Path, location: String): String = {
    val path = localPath(location)
    require(path.startsWith(root), s"File outside data location: $location, root=$root")
    root.relativize(path).toString
  }

  def physical(root: Path): Set[String] = IcebergTestFiles
    .regularFiles(root)
    // Hadoop LocalFileSystem CRC sidecars are metadata, not native data files.
    .filterNot { path =>
      val name = Paths.get(path).getFileName.toString
      name.startsWith(".") && name.endsWith(".crc")
    }

  def context(): SchedulerAttempt = {
    val tc = TaskContext.get()
    SchedulerAttempt(
      tc.stageId(),
      tc.stageAttemptNumber(),
      tc.partitionId(),
      tc.taskAttemptId(),
      tc.attemptNumber(),
      SparkEnv.get.executorId,
      SparkEnv.get.blockManager.blockManagerId.host,
      ManagementFactory.getRuntimeMXBean.getName.split("@")(0).toLong)
  }
}

/** All executor state is shipped in the task closure; the files are the cross-JVM channel. */
private[comet] case class SharedIcebergSchedulerProbe(
    directory: String,
    mode: String,
    timeoutMillis: Long)
    extends IcebergSchedulerTestProbe {
  import IcebergSchedulerFiles._

  private def root: Path = Paths.get(directory)
  private def attemptDir(a: SchedulerAttempt): Path = root.resolve(s"attempt-${a.attempt}")

  override def beforeInput(): Unit = {
    val a = context()
    val dir = attemptDir(a)
    write(dir.resolve("start.json"), a)
    TaskContext.get().addTaskCompletionListener[Unit] { _ =>
      write(dir.resolve("complete.json"), a)
    }
    TaskContext.get().addTaskFailureListener { (_, _) =>
      write(dir.resolve("failure.json"), a)
    }
    // Leave two reducers active. Others fetch only after loss, provoking missing shuffle
    // outputs in the stage-reexecution variant (external shuffle service must be disabled).
    if (mode == "stage-loss" && a.stageAttempt == 0 && a.partition >= 2) {
      await("release-start", timeoutMillis)(
        Files.exists(root.resolve("release-start")) || Files.exists(root.resolve("release-all")))(
        paths(root).mkString("\n"))
    }
  }

  override def beforeNative(): Option[IcebergWriteTestProbe] = {
    val a = context()
    val dir = attemptDir(a)
    if (mode.endsWith("loss") && a.number == 0 && a.stageAttempt == 0) {
      // The second 1,000-row unit rolls the first tiny file to disk. The first unit can
      // still be entirely buffered by Parquet/FileIO, so it cannot prove physical progress.
      Some(
        IcebergWriteTestProbe
          .newBuilder()
          .setAttemptDirectory(dir.toString)
          .setPauseAfterRows(2000L)
          .setTimeoutMillis(timeoutMillis)
          .build())
    } else None
  }

  override def afterHandoff(locations: Seq[String]): Unit = {
    val a = context().copy(files = locations)
    val dir = attemptDir(a)
    write(dir.resolve("handoff.json"), a)
    if (mode == "speculation" && a.partition == 0) {
      await("release-handoff", timeoutMillis)(
        Files.exists(dir.resolve("release-handoff")) || Files.exists(
          root.resolve("release-all")))(paths(root).mkString("\n"))
    }
  }

  override def accepted(partition: Int, locations: Seq[String]): Unit = {
    Files.createFile(root.resolve(s"accepted-$partition.once"))
    write(root.resolve(s"accepted-$partition.json"), Map("files" -> locations))
  }

  override def committed(): Unit = {
    Files.createFile(root.resolve("committed"))
    ()
  }
}

private[comet] case class SchedulerTaskEvent(
    stage: Int,
    stageAttempt: Int,
    partition: Int,
    attempt: Long,
    executor: String,
    host: String,
    speculative: Boolean,
    reason: String)

/** Snapshot event values immediately: Spark's TaskInfo is mutable. */
private[comet] class IcebergSchedulerEvents(group: String) extends SparkListener {
  val started = new ConcurrentLinkedQueue[SchedulerTaskEvent]()
  val ended = new ConcurrentLinkedQueue[SchedulerTaskEvent]()
  val removed = new ConcurrentLinkedQueue[String]()
  val stages = java.util.concurrent.ConcurrentHashMap.newKeySet[Integer]()
  val stageEnds = new ConcurrentLinkedQueue[String]()

  override def onJobStart(e: SparkListenerJobStart): Unit = {
    if (Option(e.properties).exists(_.getProperty("spark.jobGroup.id") == group)) {
      e.stageIds.foreach(id => stages.add(Integer.valueOf(id)))
    }
  }

  override def onTaskStart(e: SparkListenerTaskStart): Unit = {
    if (stages.contains(Integer.valueOf(e.stageId))) {
      val t = e.taskInfo
      started.add(
        SchedulerTaskEvent(
          e.stageId,
          e.stageAttemptId,
          t.partitionId,
          t.taskId,
          t.executorId,
          t.host,
          t.speculative,
          "Started"))
    }
  }

  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
    if (stages.contains(Integer.valueOf(e.stageId))) {
      val t = e.taskInfo
      ended.add(
        SchedulerTaskEvent(
          e.stageId,
          e.stageAttemptId,
          t.partitionId,
          t.taskId,
          t.executorId,
          t.host,
          t.speculative,
          e.reason.toString))
    }
  }

  override def onExecutorRemoved(e: SparkListenerExecutorRemoved): Unit = {
    removed.add(s"${e.executorId}: ${e.reason}")
  }

  override def onStageCompleted(e: SparkListenerStageCompleted): Unit = {
    if (stages.contains(Integer.valueOf(e.stageInfo.stageId))) {
      stageEnds.add(
        s"${e.stageInfo.stageId}/${e.stageInfo.attemptNumber()}: " +
          e.stageInfo.failureReason.toString)
    }
  }

  def removedExecutor(id: String): Boolean = removed.asScala.exists(_.startsWith(id + ":"))

  def diagnostic: String =
    s"started=${started.asScala.toVector}\nended=${ended.asScala.toVector}\n" +
      s"removed=${removed.asScala.toVector}\nstages=${stageEnds.asScala.toVector}"
}

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

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ExecutorService, ThreadFactory}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.types.StructType

import org.apache.comet.CometConf
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.iceberg.compaction.IcebergRewriteReflection.{RewriteGroup, RewritePlan}

/**
 * Comet's bin-pack `rewrite_data_files`. Iceberg plans and commits the rewrite, with the classes
 * of its own `RewriteDataFilesSparkAction`, and Comet runs the Spark jobs in between:
 *
 *   - Iceberg's `BinPackRewriteFilePlanner` plans the file groups of the current snapshot.
 *   - Comet rewrites all the groups of a commit in one Spark job. Each Spark task reads one bin
 *     of a group with Comet's native Iceberg scan and writes it back with Iceberg's rewrite
 *     write, which Comet runs with its native Iceberg writer when that is enabled. The bins of a
 *     group are the task groups `TableScanUtil.planTaskGroups` makes of it, which is the call
 *     Iceberg's staged scan makes, so a bin is a Spark partition of Iceberg's own job for the
 *     group.
 *   - Iceberg's `RewriteDataFilesCommitManager` commits the groups, in one snapshot or, with
 *     `partial-progress.enabled`, in several.
 *
 * Iceberg's action runs a Spark job per group, and Comet's native scan already reads those jobs
 * through Iceberg's staged scan. What a table with many small groups loses there is the jobs
 * themselves: `max-concurrent-file-group-rewrites` small jobs at a time leave most of the cluster
 * idle. A call that Comet cannot run as one job per commit is therefore left to Iceberg's
 * procedure, and [[prepare]] decides that before anything is written.
 */
object CometRewriteDataFiles extends Logging {

  /** What a `CALL` hands to the rewrite. */
  final case class Request(
      session: SparkSession,
      table: AnyRef,
      options: Map[String, String],
      filter: Option[AnyRef],
      refreshCache: () => Unit)

  /** The columns of the procedure's result row. */
  final case class Result(
      rewrittenDataFilesCount: Int,
      addedDataFilesCount: Int,
      rewrittenBytesCount: Long,
      failedDataFilesCount: Int,
      removedDeleteFilesCount: Int)

  object Result {
    val Empty: Result = Result(0, 0, 0L, 0, 0)
  }

  /**
   * The most Spark partitions one job reads. Every task returns its commit message to the driver,
   * which holds them all until the job ends, and `spark.driver.maxResultSize` limits what they
   * add up to. A table with wide statistics has messages of tens of kilobytes, so this keeps a
   * job well under the default limit of 1 GiB.
   */
  private[compaction] val MaxBinsPerJob = 10000

  /** The table property of the lookback Iceberg's staged scan plans its read with. */
  private val SplitLookback = "read.split.planning-lookback"
  private val SplitLookbackDefault = 10

  /** The Spark job group id prefix of the jobs that rewrite file groups. */
  val JobGroupPrefix = "COMET-REWRITE-DATA-FILES"

  private val jobCounter = new AtomicInteger(0)

  /**
   * Plans a rewrite without changing anything. Returns the reason when Comet leaves the call to
   * Iceberg, which the caller must then run instead.
   */
  def prepare(request: Request): Either[String, PreparedRewrite] = {
    val table = request.table
    IcebergRewriteReflection.currentSnapshotId(table) match {
      case None =>
        // An empty table has nothing to rewrite, and Iceberg does not look at the options.
        Right(new PreparedRewrite(request, None))
      case Some(snapshotId) =>
        val caseSensitive = request.session.sessionState.conf.caseSensitiveAnalysis
        val planner =
          IcebergRewriteReflection.newPlanner(table, request.filter, snapshotId, caseSensitive)
        for {
          options <- CompactionOptions.parse(
            request.options,
            IcebergRewriteReflection.plannerOptions(planner))
          plan <- planGroups(planner, request.options)
          rewrite <- planRewrite(request, snapshotId, options, plan)
        } yield new PreparedRewrite(request, rewrite)
    }
  }

  /** Iceberg's plan, or the reason Iceberg's planner rejected one of its options. */
  private def planGroups(
      planner: AnyRef,
      options: Map[String, String]): Either[String, RewritePlan] =
    try Right(IcebergRewriteReflection.plan(planner, options))
    catch {
      case e: IllegalArgumentException => Left(s"Iceberg rejects the options: ${e.getMessage}")
    }

  /**
   * The jobs that rewrite `plan`, or the reason Comet leaves the call to Iceberg. Every job's
   * read is planned here, which writes nothing, so that a read Comet's native scan does not take
   * over sends the whole call to Iceberg.
   */
  private def planRewrite(
      request: Request,
      snapshotId: Long,
      options: CompactionOptions,
      plan: RewritePlan): Either[String, Option[Rewrite]] = {
    val table = request.table
    val conf = request.session.sessionState.conf
    lazy val schema = IcebergReflection.getSchema(table).map(_.asInstanceOf[AnyRef])
    lazy val lookback = IcebergRewriteReflection
      .tableProperties(table)
      .get(SplitLookback)
      .map(_.toInt)
      .getOrElse(SplitLookbackDefault)

    if (plan.groups.isEmpty) {
      Right(None)
    } else if (!CometConf.COMET_EXEC_ENABLED.get(conf) ||
      !CometConf.COMET_ICEBERG_NATIVE_ENABLED.get(conf)) {
      Left("Comet's native Iceberg scan is disabled")
    } else if (IcebergReflection.getFormatVersion(table).forall(_ >= 3)) {
      // Iceberg's rewrite of a table with row lineage reads and writes the lineage columns,
      // which only Iceberg's own staged scan provides.
      Left("the table has row lineage, which only Iceberg's staged scan reads")
    } else if (schema.isEmpty) {
      Left("Comet cannot read the table's schema")
    } else if (lookback <= 0) {
      Left(s"'$SplitLookback' is $lookback")
    } else if (!plan.groups.forall(IcebergRewriteReflection.inOutputSpec)) {
      // Iceberg's write repartitions such a group, and a bin is written as it is read.
      Left("a file group has files outside the output partition spec")
    } else {
      val work = configuredSession(request.session)
      planJobs(work, table, schema.get, lookback, options, plan).map { jobs =>
        logInfo(
          s"Comet rewrites ${plan.groups.size} file groups of ${tableName(table)} in " +
            s"${jobs.size} jobs")
        Some(new Rewrite(table, work, snapshotId, options, plan, jobs))
      }
    }
  }

  /** The jobs of `plan`, or the reason Comet's native scan does not read all of them. */
  private def planJobs(
      work: SparkSession,
      table: AnyRef,
      icebergSchema: AnyRef,
      lookback: Int,
      options: CompactionOptions,
      plan: RewritePlan): Either[String, Seq[BinsJob]] = {
    val bins = plan.groups.map { group =>
      group -> IcebergRewriteReflection
        .planTaskGroups(group.tasks, group.inputSplitSize, lookback, openFileCost = 0L)
        .map(new BinTasks(_))
    }
    // With partial progress a job covers the groups of one commit, so that a failed job fails
    // one commit.
    val groupsPerJob =
      if (options.partialProgress) {
        RewriteJobs.groupsPerCommit(plan.totalGroupCount, options.maxCommits)
      } else {
        Int.MaxValue
      }
    val sparkSchema = IcebergReflection.toSparkSchema(icebergSchema)
    val jobs = mutable.ArrayBuffer.empty[BinsJob]
    try {
      RewriteJobs.chunk(bins, groupsPerJob, MaxBinsPerJob)(_._2.size).foreach { chunk =>
        jobs += binsJob(work, table, icebergSchema, sparkSchema, chunk)
      }
    } catch {
      case NonFatal(e) =>
        jobs.foreach(job => CompactionBatches.remove(job.batchId))
        throw e
    }
    if (jobs.forall(_.readByNativeScan)) {
      Right(jobs.toVector)
    } else {
      jobs.foreach(job => CompactionBatches.remove(job.batchId))
      Left("Comet's native Iceberg scan does not read every file group")
    }
  }

  /**
   * A job that reads the bins of `chunk` through [[CompactionSource]]. Spark cannot read a
   * [[CompactionScan]], so the read is planned here to see whether Comet's native scan takes it
   * over.
   */
  private def binsJob(
      work: SparkSession,
      table: AnyRef,
      icebergSchema: AnyRef,
      sparkSchema: StructType,
      chunk: Seq[(RewriteGroup, Seq[BinTasks])]): BinsJob = {
    val batchId = UUID.randomUUID().toString
    val bins = chunk.flatMap(_._2)
    val batch = new CompactionBatch(table, icebergSchema, sparkSchema, bins)
    CompactionBatches.register(batchId, batch)
    try {
      val scan = work.read
        .format(classOf[CompactionSource].getName)
        .option(CompactionSource.BatchIdOption, batchId)
        .load()
      val native =
        scan.queryExecution.executedPlan.exists(_.isInstanceOf[CometIcebergNativeScanExec])
      BinsJob(chunk.map(_._1), batchId, scan, bins.size, native)
    } catch {
      case NonFatal(e) =>
        CompactionBatches.remove(batchId)
        throw e
    }
  }

  /**
   * A copy of the session for the rewrite's own jobs. Adaptive execution is off because it can
   * change the output partitioning of the write, and delete files are not cached on executors
   * because every partition is rewritten once. Iceberg's action does the same.
   */
  private def configuredSession(session: SparkSession): SparkSession = {
    val work = session.getClass
      .getMethod("cloneSession")
      .invoke(session)
      .asInstanceOf[SparkSession]
    work.conf.set("spark.sql.adaptive.enabled", "false")
    work.conf.set("spark.sql.iceberg.executor-cache.delete-files.enabled", "false")
    work
  }

  private def tableName(table: AnyRef): String = IcebergRewriteReflection.tableName(table)

  /**
   * One Spark job of a rewrite: several file groups, read through [[CompactionSource]] with one
   * Spark partition per bin.
   *
   * @param scan
   *   the DataFrame that reads the bins, planned already
   * @param readByNativeScan
   *   whether Comet's native Iceberg scan took the read over
   */
  private[compaction] final case class BinsJob(
      groups: Seq[RewriteGroup],
      batchId: String,
      scan: DataFrame,
      binCount: Int,
      readByNativeScan: Boolean) {

    def inputFiles: Int = groups.map(_.inputFileNum).sum
  }

  /** The default executor, daemon threads so a stuck job never keeps the JVM alive. */
  private def newPool(size: Int): ExecutorService =
    Executors.newFixedThreadPool(
      math.max(1, size),
      new ThreadFactory {
        private val counter = new AtomicInteger(0)
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, s"Comet-Rewrite-Service-${counter.getAndIncrement()}")
          thread.setDaemon(true)
          thread
        }
      })

  /** A rewrite that has been planned and is ready to run, or None when there is nothing to do. */
  final class PreparedRewrite private[compaction] (request: Request, rewrite: Option[Rewrite]) {

    def execute(): Result = {
      val result = rewrite match {
        case Some(r) =>
          try r.run()
          finally r.release()
        case None =>
          logInfo(s"Nothing found to rewrite in ${tableName(request.table)}")
          Result.Empty
      }
      request.refreshCache()
      result
    }
  }

  /**
   * The jobs of a rewrite, and their commits.
   *
   * @param snapshotId
   *   the snapshot the plan was read from, which the commits validate against
   */
  private[compaction] final class Rewrite(
      table: AnyRef,
      work: SparkSession,
      snapshotId: Long,
      options: CompactionOptions,
      plan: RewritePlan,
      jobs: Seq[BinsJob]) {

    def run(): Result = {
      val commitManager = IcebergRewriteReflection.commitManager(
        table,
        snapshotId,
        options.useStartingSequenceNumber)
      val result =
        if (options.partialProgress) runWithPartialProgress(commitManager)
        else runAtomic(commitManager)
      if (options.removeDanglingDeletes) {
        val removed = IcebergRewriteReflection.removeDanglingDeletes(work, table)
        result.copy(removedDeleteFilesCount = result.removedDeleteFilesCount + removed)
      } else {
        result
      }
    }

    /** Drops the batches of the jobs. A job that ran has dropped its own already. */
    def release(): Unit = jobs.foreach(job => CompactionBatches.remove(job.batchId))

    // -- Atomic ----------------------------------------------------------------------------

    private def runAtomic(commitManager: AnyRef): Result = {
      val rewritten = new ConcurrentLinkedQueue[BinsJob]()
      val failure = new AtomicReference[Throwable](null)
      runJobs { job =>
        // Once a job has failed, jobs that have not started are skipped.
        if (failure.get() == null) {
          try {
            rewrite(job)
            val _ = rewritten.add(job)
          } catch {
            case e: Throwable =>
              logWarning(s"Failure during rewrite process for ${describe(job)}", e)
              val _ = failure.compareAndSet(null, e)
          }
        }
      }

      val groups = rewritten.asScala.toSeq.flatMap(_.groups)
      if (failure.get() != null) {
        logError(
          s"Cannot complete rewrite, ${CompactionOptions.PartialProgressEnabled} is not " +
            "enabled and one of the file groups failed to be rewritten. This error occurred " +
            "during the writing of new files, not during the commit process. Cleaning up " +
            s"${groups.size} groups which finished being written.")
        groups.foreach(IcebergRewriteReflection.abortFileGroup(commitManager, _))
        throw failure.get()
      }

      try {
        IcebergRewriteReflection.commitOrClean(commitManager, groups)
      } catch {
        case e: Exception if IcebergRewriteReflection.isCommitConflict(e) =>
          throw new RuntimeException(
            "Cannot commit rewrite because of a ValidationException or CommitFailedException. " +
              "This usually means that this rewrite has conflicted with another concurrent " +
              "Iceberg operation. To reduce the likelihood of conflicts, set " +
              s"${CompactionOptions.PartialProgressEnabled} which will break up the rewrite " +
              s"into multiple smaller commits controlled by " +
              s"${CompactionOptions.PartialProgressMaxCommits}. Separate smaller rewrite " +
              "commits can succeed independently while any commits that conflict with " +
              "another Iceberg operation will be ignored. This mode will create additional " +
              "snapshots in the table history, one for each commit.",
            e)
      }
      resultOf(groups, failedFiles = 0)
    }

    // -- Partial progress ------------------------------------------------------------------

    private def runWithPartialProgress(commitManager: AnyRef): Result = {
      val committer = new BatchCommitter(
        RewriteJobs.groupsPerCommit(plan.totalGroupCount, options.maxCommits),
        batch => IcebergRewriteReflection.commitOrClean(commitManager, batch.flatMap(_.groups)))
      val failedFiles = new AtomicInteger(0)
      runJobs { job =>
        try {
          rewrite(job)
          committer.offer(job)
        } catch {
          case e: Exception =>
            logError(s"Failure during rewrite of ${describe(job)}", e)
            val _ = failedFiles.addAndGet(job.inputFiles)
        }
      }
      committer.finish()

      val totalCommits = math.min(plan.totalGroupCount, options.maxCommits)
      val failedCommits = totalCommits - committer.succeededCommits
      if (failedCommits > 0 && failedCommits <= options.maxFailedCommits) {
        logWarning(
          s"${CompactionOptions.PartialProgressEnabled} is true but $failedCommits rewrite " +
            "commits failed. Check the logs to determine why the individual commits failed. " +
            s"If this is persistent it may help to increase " +
            s"${CompactionOptions.PartialProgressMaxCommits} which will split the rewrite " +
            "operation into smaller commits.")
      } else if (failedCommits > options.maxFailedCommits) {
        throw new RuntimeException(
          s"${CompactionOptions.PartialProgressEnabled} is true but $failedCommits rewrite " +
            "commits failed. This is more than the maximum allowed failures of " +
            s"${options.maxFailedCommits}. Check the logs to determine why the individual " +
            "commits failed. If this is persistent it may help to increase " +
            s"${CompactionOptions.PartialProgressMaxCommits} which will split the rewrite " +
            "operation into smaller commits.")
      }
      resultOf(committer.committedJobs.flatMap(_.groups), failedFiles.get())
    }

    // -- Running a job ---------------------------------------------------------------------

    /** Runs `body` for every job, `max-concurrent-file-group-rewrites` of them at a time. */
    private def runJobs(body: BinsJob => Unit): Unit = {
      val pool = newPool(math.min(options.maxConcurrentGroupRewrites, jobs.size))
      try {
        val futures = jobs.map { job =>
          pool.submit(new Runnable {
            override def run(): Unit = body(job)
          })
        }
        futures.foreach(_.get())
      } finally {
        pool.shutdown()
      }
    }

    /**
     * Rewrites the groups of a job in one Spark job. Every Spark task reads one bin and writes it
     * back, so a bin of about the target file size becomes about one file. The groups share the
     * output settings, which Iceberg's planner takes from the options and not from the group.
     */
    private def rewrite(job: BinsJob): Unit = {
      val first = job.groups.head
      val description = s"Rewriting ${job.inputFiles} files (BIN-PACK, Comet, " +
        s"${job.groups.size} file groups, ${job.binCount} read tasks) in ${tableName(table)}"
      withJobGroup(description) {
        try {
          IcebergRewriteReflection.cacheTable(table, job.batchId)
          job.scan.write
            .format("iceberg")
            .option("rewritten-file-scan-task-set-id", job.batchId)
            .option("target-file-size-bytes", first.maxOutputFileSize.toString)
            .option("distribution-mode", "none")
            .option("output-spec-id", first.outputSpecId.toString)
            .mode("append")
            .save(job.batchId)
          // The groups of a job are always committed together, so the files the job wrote go to
          // its first group and the commit adds them with the files of every group.
          IcebergRewriteReflection.setOutputFiles(
            first,
            IcebergRewriteReflection.newFiles(table, job.batchId))
          logInfo(s"Rewrite files ready to be committed - $description")
        } finally {
          IcebergRewriteReflection.release(table, job.batchId)
          CompactionBatches.remove(job.batchId)
        }
      }
    }

    /** Runs `body` under a job group of its own, so that the Spark UI shows what it rewrites. */
    private def withJobGroup[T](description: String)(body: => T): T = {
      val sparkContext = work.sparkContext
      // Iceberg names its jobs REWRITE-DATA-FILES, so the prefix shows who ran the rewrite.
      sparkContext.setJobGroup(
        s"$JobGroupPrefix-${jobCounter.incrementAndGet()}",
        description,
        interruptOnCancel = false)
      try {
        body
      } finally {
        sparkContext.clearJobGroup()
      }
    }

    private def resultOf(groups: Seq[RewriteGroup], failedFiles: Int): Result = {
      val results = groups.map(IcebergRewriteReflection.groupResult)
      Result(
        rewrittenDataFilesCount = results.map(_.rewrittenDataFiles).sum,
        addedDataFilesCount = results.map(_.addedDataFiles).sum,
        rewrittenBytesCount = results.map(_.rewrittenBytes).sum,
        failedDataFilesCount = failedFiles,
        removedDeleteFilesCount = results.map(_.removedDeleteFiles).sum)
    }

    private def describe(job: BinsJob): String =
      s"the ${job.groups.size} file groups of job ${job.batchId}"
  }

  /**
   * Commits finished jobs in batches of at least `groupsPerCommit` groups while the others are
   * still running, the way iceberg-java's commit service does for `partial-progress.enabled`. A
   * batch takes whole jobs, because the groups of a job share its files. A failed batch is logged
   * and dropped, and Iceberg's commit manager deletes its files.
   */
  private final class BatchCommitter(groupsPerCommit: Int, commit: Seq[BinsJob] => Unit) {
    private val pending = new java.util.ArrayDeque[BinsJob]()
    private var pendingGroups = 0
    private val committed = new ConcurrentLinkedQueue[BinsJob]()
    private val succeeded = new AtomicInteger(0)

    /** Serializes commits, which iceberg-java's partial-progress service does not guarantee. */
    private val commitLock = new Object

    def succeededCommits: Int = succeeded.get()

    def committedJobs: Seq[BinsJob] = committed.asScala.toSeq

    def offer(job: BinsJob): Unit = {
      pending.synchronized {
        pending.add(job)
        pendingGroups += job.groups.size
      }
      commitReadyBatches(finalPass = false)
    }

    /** Commits what is left once every job has finished. */
    def finish(): Unit = commitReadyBatches(finalPass = true)

    private def commitReadyBatches(finalPass: Boolean): Unit = commitLock.synchronized {
      var batch = nextBatch(finalPass)
      while (batch.nonEmpty) {
        try {
          commit(batch)
          batch.foreach(committed.add)
          succeeded.incrementAndGet()
        } catch {
          case e: Exception =>
            logError(
              "Failure during rewrite commit process, partial progress enabled. Ignoring",
              e)
        }
        batch = nextBatch(finalPass)
      }
    }

    private def nextBatch(finalPass: Boolean): Seq[BinsJob] = pending.synchronized {
      if (pendingGroups >= groupsPerCommit || (finalPass && !pending.isEmpty)) {
        val batch = Seq.newBuilder[BinsJob]
        var taken = 0
        while (taken < groupsPerCommit && !pending.isEmpty) {
          val next = pending.poll()
          pendingGroups -= next.groups.size
          batch += next
          taken += next.groups.size
        }
        batch.result()
      } else {
        Nil
      }
    }
  }
}

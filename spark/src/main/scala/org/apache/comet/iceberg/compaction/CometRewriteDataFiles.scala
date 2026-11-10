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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.types.StructType

import org.apache.comet.CometConf
import org.apache.comet.iceberg.IcebergReflection

/**
 * Comet's bin-pack `rewrite_data_files`. It follows iceberg-java's `RewriteDataFilesSparkAction`
 * step for step, so that a call plans the same groups either way:
 *
 *   1. Plan the data files of the current snapshot into file groups ([[BinPackPlanner]]).
 *   1. Rewrite the groups in Spark jobs, which read the files with Comet's native Iceberg scan
 *      and write them back with Iceberg's rewrite write, which Comet runs with its native Iceberg
 *      writer when `spark.comet.write.iceberg.enabled` is set. Up to
 *      `max-concurrent-file-group-rewrites` jobs run at once.
 *   1. Commit the rewritten groups as one `RewriteFiles` snapshot, or in several snapshots with
 *      `partial-progress.enabled`.
 *
 * Iceberg runs a job per group. Each job reads the group's files through Iceberg's staged scan,
 * which packs them into Spark partitions of about the target file size. A table with many
 * partitions has many small groups, so only a few Spark tasks run at once. A job here covers
 * every group of a commit, with one Spark partition per bin the planner chose, so all of the
 * commit's bins run together. A group that cannot be read this way is rewritten as Iceberg does.
 *
 * The data movement stays in Iceberg's write path on purpose. It carries everything that must
 * match iceberg-java (metrics, partition values, sort order ids, file naming), and the
 * eligibility gates that fall back to Iceberg's own writer when Comet cannot match it.
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
   * Plans a rewrite without changing anything. Returns the reason when Comet leaves the call to
   * Iceberg, which the caller must then run instead.
   */
  def prepare(request: Request): Either[String, PreparedRewrite] = {
    val table = request.table
    IcebergRewriteReflection.currentSnapshotId(table) match {
      case None =>
        // An empty table has nothing to rewrite, and Iceberg does not look at the options.
        Right(new PreparedRewrite(request, 0L, None, CompactionPlan(Nil, 0)))
      case Some(startingSnapshotId) =>
        val specId = IcebergRewriteReflection.currentSpecId(table)
        CompactionOptions.parse(
          request.options,
          IcebergRewriteReflection.tableProperties(table),
          specId) match {
          case Left(reason) => Left(reason)
          case Right(options) =>
            val files = IcebergRewriteReflection.planFiles(
              request.session,
              table,
              startingSnapshotId,
              request.filter,
              specId)
            val plan = BinPackPlanner.plan(files, options)
            Right(new PreparedRewrite(request, startingSnapshotId, Some(options), plan))
        }
    }
  }

  /** Groups whose files have been rewritten and not yet committed. */
  private[compaction] final case class RewrittenGroups(
      groups: Seq[FileGroup],
      addedFiles: Seq[AnyRef],
      danglingDeletionVectors: Seq[AnyRef])

  /** One Spark job of a rewrite. */
  private sealed trait RewriteJob {
    def groups: Seq[FileGroup]
  }

  /** A single group, read through Iceberg's staged scan the way Iceberg's action reads it. */
  private final case class StagedJob(group: FileGroup) extends RewriteJob {
    override def groups: Seq[FileGroup] = Seq(group)
  }

  /**
   * Several groups, read through [[CompactionSource]] with one Spark partition per bin.
   *
   * @param scan
   *   the DataFrame that reads the bins. It is planned already, and Comet's native scan has taken
   *   it over.
   */
  private final case class BinsJob(
      groups: Seq[FileGroup],
      batchId: String,
      scan: DataFrame,
      binCount: Int)
      extends RewriteJob

  /** What a bins job needs from the table, which does not change between jobs. */
  private final case class BinsSupport(
      icebergSchema: AnyRef,
      sparkSchema: StructType,
      lookback: Int)

  /**
   * The most Spark partitions one bins job reads. Every task returns its commit message to the
   * driver, which holds them all until the job ends, and `spark.driver.maxResultSize` limits what
   * they add up to. A table with wide statistics has messages of tens of kilobytes, so this keeps
   * a job well under the default limit of 1 GiB.
   */
  private[compaction] val MaxBinsPerJob = 10000

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

  /** The Spark job group id prefix of the jobs that rewrite file groups. */
  val JobGroupPrefix = "COMET-REWRITE-DATA-FILES"

  private val jobCounter = new AtomicInteger(0)

  /**
   * A rewrite that has been planned and is ready to run.
   *
   * @param snapshotId
   *   the snapshot the plan was read from, which commits validate against. Unused when the table
   *   has no snapshot.
   * @param options
   *   None only when the table has no snapshot
   */
  final class PreparedRewrite private[compaction] (
      request: Request,
      snapshotId: Long,
      options: Option[CompactionOptions],
      plan: CompactionPlan) {

    private val table = request.table

    /** The batches registered for bins jobs, which are released when the rewrite ends. */
    private val registeredBatches = new ConcurrentLinkedQueue[String]()

    def groupCount: Int = plan.groups.size

    def execute(): Result = {
      val result = options match {
        case Some(opts) if plan.groups.nonEmpty =>
          val work = configuredSession()
          try {
            if (opts.partialProgress) {
              executeWithPartialProgress(work, opts)
            } else {
              executeAtomic(work, opts)
            }
          } finally {
            releaseBatches()
          }
        case _ =>
          logInfo(s"Nothing found to rewrite in ${IcebergRewriteReflection.tableName(table)}")
          Result.Empty
      }
      request.refreshCache()
      result
    }

    /**
     * A copy of the session for the rewrite's own jobs. Adaptive execution is off because it can
     * change the output partitioning of the write, and delete files are not cached on executors
     * because every partition is rewritten once. Iceberg's action does the same.
     */
    private def configuredSession(): SparkSession = {
      val work = request.session.getClass
        .getMethod("cloneSession")
        .invoke(request.session)
        .asInstanceOf[SparkSession]
      work.conf.set("spark.sql.adaptive.enabled", "false")
      work.conf.set("spark.sql.iceberg.executor-cache.delete-files.enabled", "false")
      work
    }

    // -- Atomic ----------------------------------------------------------------------------

    private def executeAtomic(work: SparkSession, opts: CompactionOptions): Result = {
      val jobs = planJobs(work, plan.groups, groupsPerJob = Int.MaxValue)
      val pool = newPool(math.min(opts.maxConcurrentGroupRewrites, jobs.size))
      val rewritten = new ConcurrentLinkedQueue[RewrittenGroups]()
      val failure = new AtomicReference[Throwable](null)
      try {
        val futures = jobs.map { job =>
          pool.submit(new Runnable {
            override def run(): Unit = {
              // Once a job has failed, jobs that have not started are skipped.
              if (failure.get() == null) {
                try {
                  val _ = rewritten.add(runJob(work, job))
                } catch {
                  case e: Throwable =>
                    logWarning(s"Failure during rewrite process for ${describe(job)}", e)
                    val _ = failure.compareAndSet(null, e)
                }
              }
            }
          })
        }
        futures.foreach(_.get())
      } finally {
        pool.shutdown()
      }

      val completed = rewritten.asScala.toSeq
      if (failure.get() != null) {
        logError(
          s"Cannot complete rewrite, ${CompactionOptions.PartialProgressEnabled} is not " +
            "enabled and one of the file groups failed to be rewritten. This error occurred " +
            "during the writing of new files, not during the commit process. Cleaning up " +
            s"${completed.map(_.groups.size).sum} groups which finished being written.")
        completed.foreach(abort)
        throw failure.get()
      }

      try {
        commitOrClean(completed, opts)
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
      resultOf(completed, failedFiles = 0)
    }

    // -- Partial progress ------------------------------------------------------------------

    private def executeWithPartialProgress(
        work: SparkSession,
        opts: CompactionOptions): Result = {
      val groupsPerCommit = math.max(1, ceilDiv(plan.totalGroupCount, opts.maxCommits))
      // A job covers at most the groups of one commit, so that a failed job fails one commit.
      val jobs = planJobs(work, plan.groups, groupsPerJob = groupsPerCommit)
      val pool = newPool(math.min(opts.maxConcurrentGroupRewrites, jobs.size))
      val committer = new BatchCommitter(groupsPerCommit, batch => commitOrClean(batch, opts))
      val failedFiles = new AtomicInteger(0)
      try {
        val futures = jobs.map { job =>
          pool.submit(new Runnable {
            override def run(): Unit = {
              try {
                committer.offer(runJob(work, job))
              } catch {
                case e: Exception =>
                  logError(s"Failure during rewrite of ${describe(job)}", e)
                  val _ = failedFiles.addAndGet(job.groups.map(_.numFiles).sum)
              }
            }
          })
        }
        futures.foreach(_.get())
      } finally {
        pool.shutdown()
      }
      committer.finish()

      val totalCommits = math.min(plan.totalGroupCount, opts.maxCommits)
      val failedCommits = totalCommits - committer.succeededCommits
      if (failedCommits > 0 && failedCommits <= opts.maxFailedCommits) {
        logWarning(
          s"${CompactionOptions.PartialProgressEnabled} is true but $failedCommits rewrite " +
            "commits failed. Check the logs to determine why the individual commits failed. " +
            s"If this is persistent it may help to increase " +
            s"${CompactionOptions.PartialProgressMaxCommits} which will split the rewrite " +
            "operation into smaller commits.")
      } else if (failedCommits > opts.maxFailedCommits) {
        throw new RuntimeException(
          s"${CompactionOptions.PartialProgressEnabled} is true but $failedCommits rewrite " +
            "commits failed. This is more than the maximum allowed failures of " +
            s"${opts.maxFailedCommits}. Check the logs to determine why the individual " +
            "commits failed. If this is persistent it may help to increase " +
            s"${CompactionOptions.PartialProgressMaxCommits} which will split the rewrite " +
            "operation into smaller commits.")
      }
      resultOf(committer.committedGroups, failedFiles.get())
    }

    // -- Planning the jobs -----------------------------------------------------------------

    /**
     * Decides how the groups are rewritten. Groups that can be read as bins are collected into
     * jobs of at most `groupsPerJob` groups and [[MaxBinsPerJob]] bins, and each other group gets
     * a job of its own. Nothing is written here, so a bins job that cannot run is replaced by its
     * groups' own jobs.
     */
    private def planJobs(
        work: SparkSession,
        groups: Seq[FileGroup],
        groupsPerJob: Int): Seq[RewriteJob] = {
      binsSupport(work) match {
        case None => groups.map(StagedJob)
        case Some(support) =>
          // A group whose files are not in the output spec is repartitioned by the write, which
          // the bins job does not do.
          val (direct, repartitioned) =
            groups.partition(group => group.files.head.specId == group.outputSpecId)
          // A group whose tasks cannot be split and packed here is left to Iceberg's staged scan.
          val (planned, unplanned) =
            direct.foldLeft((Vector.empty[(FileGroup, Seq[BinTasks])], Vector.empty[FileGroup])) {
              case ((ok, failed), group) =>
                try {
                  (ok :+ (group -> binsOf(group, support.lookback)), failed)
                } catch {
                  case NonFatal(e) =>
                    logInfo(s"Comet cannot plan the read of ${describe(group)}: ${e.getMessage}")
                    (ok, failed :+ group)
                }
            }
          val jobs =
            BinPackPlanner.chunk(planned, groupsPerJob, MaxBinsPerJob)(_._2.size).flatMap {
              chunked =>
                binsJob(work, support, chunked) match {
                  case Some(job) => Seq(job)
                  case None => chunked.map { case (group, _) => StagedJob(group) }
                }
            }
          jobs ++ (unplanned ++ repartitioned).map(StagedJob)
      }
    }

    /**
     * What a bins job needs, or None when the table or the session cannot use one. A table with
     * row lineage reads and writes the lineage columns in a rewrite, which only Iceberg's own
     * staged scan provides.
     */
    private def binsSupport(work: SparkSession): Option[BinsSupport] = {
      val conf = work.sessionState.conf
      if (!CometConf.COMET_ICEBERG_COMPACTION_SINGLE_JOB_ENABLED.get(conf) ||
        !CometConf.COMET_ICEBERG_NATIVE_ENABLED.get(conf) ||
        !CometConf.COMET_EXEC_ENABLED.get(conf)) {
        None
      } else {
        try {
          if (IcebergReflection.getFormatVersion(table).exists(_ < 3)) {
            val lookback = IcebergRewriteReflection
              .tableProperties(table)
              .get("read.split.planning-lookback")
              .map(_.toInt)
              .getOrElse(BinPackPlanner.DefaultSplitLookback)
            val schema = IcebergReflection.getSchema(table).map(_.asInstanceOf[AnyRef])
            if (lookback > 0 && schema.isDefined) {
              Some(BinsSupport(schema.get, IcebergReflection.toSparkSchema(schema.get), lookback))
            } else {
              None
            }
          } else {
            None
          }
        } catch {
          case NonFatal(e) =>
            logInfo(s"Comet rewrites each file group in its own job: ${e.getMessage}")
            None
        }
      }
    }

    /**
     * The bins of a group: its large files split into read tasks, packed into Spark partitions of
     * about `inputSplitSize`, which is how Iceberg's staged scan plans the read.
     */
    private def binsOf(group: FileGroup, lookback: Int): Seq[BinTasks] = {
      val readTasks =
        IcebergRewriteReflection.splitTasks(group.files.map(_.task), group.inputSplitSize)
      BinPackPlanner
        .planReadTasks(readTasks, group.inputSplitSize, lookback, openFileCost = 0L)(
          _.sizeBytes,
          _.filesCount)
        .map { bin =>
          val merged = IcebergRewriteReflection.mergeTasks(bin.map(_.task))
          new BinTasks(new java.util.ArrayList[AnyRef](merged.asJava))
        }
    }

    /**
     * A job that reads the bins of `chunked`, or None when Comet's native scan does not take over
     * the read. Spark cannot read a [[CompactionScan]], so the plan is checked before the job is
     * committed to. The scan is planned here, which changes nothing.
     */
    private def binsJob(
        work: SparkSession,
        support: BinsSupport,
        chunked: Seq[(FileGroup, Seq[BinTasks])]): Option[BinsJob] = {
      val batchId = UUID.randomUUID().toString
      val batch = new CompactionBatch(
        table,
        support.icebergSchema,
        support.sparkSchema,
        chunked.flatMap(_._2))
      CompactionBatches.register(batchId, batch)
      registeredBatches.add(batchId)
      val scan =
        try {
          val df = work.read
            .format(classOf[CompactionSource].getName)
            .option(CompactionSource.BatchIdOption, batchId)
            .load()
          if (df.queryExecution.executedPlan.exists(_.isInstanceOf[CometIcebergNativeScanExec])) {
            Some(df)
          } else {
            logInfo(
              "Comet's native Iceberg scan does not read these file groups, so each is " +
                "rewritten in its own job")
            None
          }
        } catch {
          case NonFatal(e) =>
            logInfo(s"Comet cannot plan one job for these file groups: ${e.getMessage}")
            None
        }
      scan match {
        case Some(df) => Some(BinsJob(chunked.map(_._1), batchId, df, batch.bins.size))
        case None =>
          CompactionBatches.remove(batchId)
          None
      }
    }

    private def releaseBatches(): Unit =
      registeredBatches.asScala.foreach { batchId =>
        CompactionBatches.remove(batchId)
        IcebergRewriteReflection.release(table, batchId)
      }

    // -- Running a job ---------------------------------------------------------------------

    private def runJob(work: SparkSession, job: RewriteJob): RewrittenGroups = job match {
      case StagedJob(group) => rewriteStaged(work, group)
      case bins: BinsJob => rewriteBins(work, bins)
    }

    /**
     * Rewrites one group, which is what iceberg-java's bin-pack runner does: read the group's
     * files in splits of `inputSplitSize`, and write them back so that each split becomes a new
     * file. A shuffle is added only when the files are not in the output spec.
     */
    private def rewriteStaged(work: SparkSession, group: FileGroup): RewrittenGroups = {
      val groupId = UUID.randomUUID().toString
      val description = s"Rewriting ${group.numFiles} files (BIN-PACK, Comet, file group " +
        s"${group.globalIndex}/${plan.totalGroupCount}) in " +
        IcebergRewriteReflection.tableName(table)
      withJobGroup(work, description) {
        try {
          val tasks = new java.util.ArrayList[AnyRef](group.numFiles)
          group.files.foreach(f => tasks.add(f.task))
          IcebergRewriteReflection.stage(table, groupId, tasks)

          val repartition = group.files.head.specId != group.outputSpecId
          work.read
            .format("iceberg")
            .option("scan-task-set-id", groupId)
            .option("split-size", group.inputSplitSize.toString)
            .option("file-open-cost", "0")
            .load(groupId)
            .write
            .format("iceberg")
            .option("rewritten-file-scan-task-set-id", groupId)
            .option("target-file-size-bytes", group.maxOutputFileSize.toString)
            .option("distribution-mode", if (repartition) "range" else "none")
            .option("output-spec-id", group.outputSpecId.toString)
            .mode("append")
            .save(groupId)

          val added = IcebergRewriteReflection.newFiles(table, groupId)
          val dvs = IcebergRewriteReflection.deletionVectors(group.files.map(_.task))
          logInfo(s"Rewrite files ready to be committed - $description")
          RewrittenGroups(Seq(group), added, dvs)
        } finally {
          IcebergRewriteReflection.release(table, groupId)
        }
      }
    }

    /**
     * Rewrites the groups of a bins job in one Spark job. Every Spark task reads one bin and
     * writes it back, so a bin of about the target file size becomes about one file. The groups
     * share the output settings, because those come from the options and not from the group.
     */
    private def rewriteBins(work: SparkSession, job: BinsJob): RewrittenGroups = {
      val first = job.groups.head
      val description = s"Rewriting ${job.groups.map(_.numFiles).sum} files (BIN-PACK, Comet, " +
        s"${job.groups.size} file groups, ${job.binCount} read tasks) in " +
        IcebergRewriteReflection.tableName(table)
      withJobGroup(work, description) {
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

          val added = IcebergRewriteReflection.newFiles(table, job.batchId)
          val dvs =
            IcebergRewriteReflection.deletionVectors(job.groups.flatMap(_.files.map(_.task)))
          logInfo(s"Rewrite files ready to be committed - $description")
          RewrittenGroups(job.groups, added, dvs)
        } finally {
          IcebergRewriteReflection.release(table, job.batchId)
          CompactionBatches.remove(job.batchId)
        }
      }
    }

    /** Runs `body` under a job group of its own, so that the Spark UI shows what it rewrites. */
    private def withJobGroup[T](work: SparkSession, description: String)(body: => T): T = {
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

    // -- Committing ------------------------------------------------------------------------

    /** Serializes commits, which iceberg-java's partial-progress service does not guarantee. */
    private val commitLock = new Object

    private def commitOrClean(rewritten: Seq[RewrittenGroups], opts: CompactionOptions): Unit = {
      try {
        commitLock.synchronized {
          IcebergRewriteReflection.commitRewrite(
            table,
            snapshotId,
            opts.useStartingSequenceNumber,
            rewrittenDataFiles = rewritten.flatMap(
              _.groups.flatMap(_.files.map(f => IcebergRewriteReflection.dataFileOf(f.task)))),
            addedDataFiles = rewritten.flatMap(_.addedFiles),
            danglingDeletionVectors = rewritten.flatMap(_.danglingDeletionVectors))
        }
      } catch {
        case e: Exception if IcebergRewriteReflection.isCommitStateUnknown(e) =>
          logError(
            "Commit state unknown, cannot clean up files because they may have been " +
              "committed successfully.",
            e)
          throw e
        case e: Exception =>
          if (IcebergRewriteReflection.isCleanableFailure(e)) {
            logError("Cannot commit groups, attempting to clean up written files", e)
            rewritten.foreach(abort)
          }
          throw e
      }
    }

    private def abort(rewritten: RewrittenGroups): Unit =
      IcebergRewriteReflection.deleteFiles(table, rewritten.addedFiles)

    private def resultOf(rewritten: Seq[RewrittenGroups], failedFiles: Int): Result = {
      val groups = rewritten.flatMap(_.groups)
      Result(
        rewrittenDataFilesCount = groups.map(_.numFiles).sum,
        addedDataFilesCount =
          rewritten.map(r => IcebergRewriteReflection.distinctDataFiles(r.addedFiles)).sum,
        rewrittenBytesCount = groups.map(_.inputBytes).sum,
        failedDataFilesCount = failedFiles,
        removedDeleteFilesCount = rewritten
          .map(r => IcebergRewriteReflection.distinctDeleteFiles(r.danglingDeletionVectors))
          .sum)
    }

    private def describe(job: RewriteJob): String =
      job.groups.map(describe).mkString("file groups ", ", ", "")

    private def describe(group: FileGroup): String =
      s"{globalIndex=${group.globalIndex}, partitionIndex=${group.partitionIndex}, " +
        s"partition=${group.partition.mkString("[", ", ", "]")}}"

    private def ceilDiv(a: Int, b: Int): Int = (a + b - 1) / b
  }

  /**
   * Commits finished jobs in batches of `groupsPerCommit` groups while the others are still
   * running, the way iceberg-java's commit service does for `partial-progress.enabled`. A failed
   * batch is logged and dropped, and its files are cleaned up by the commit.
   */
  private final class BatchCommitter(groupsPerCommit: Int, commit: Seq[RewrittenGroups] => Unit) {
    private val pending = new java.util.ArrayDeque[RewrittenGroups]()
    private var pendingGroups = 0
    private val committed = new ConcurrentLinkedQueue[RewrittenGroups]()
    private val succeeded = new AtomicInteger(0)

    def succeededCommits: Int = succeeded.get()

    def committedGroups: Seq[RewrittenGroups] = committed.asScala.toSeq

    def offer(rewritten: RewrittenGroups): Unit = {
      pending.synchronized {
        pending.add(rewritten)
        pendingGroups += rewritten.groups.size
      }
      commitReadyBatches(finalPass = false)
    }

    /** Commits what is left once every job has finished. */
    def finish(): Unit = commitReadyBatches(finalPass = true)

    private def commitReadyBatches(finalPass: Boolean): Unit = {
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

    private def nextBatch(finalPass: Boolean): Seq[RewrittenGroups] = pending.synchronized {
      if (pendingGroups >= groupsPerCommit || (finalPass && !pending.isEmpty)) {
        val batch = Seq.newBuilder[RewrittenGroups]
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

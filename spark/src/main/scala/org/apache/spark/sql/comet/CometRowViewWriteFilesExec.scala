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

import java.util.Date

import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{DynamicPartitionDataSingleWriter, EmptyDirectoryDataWriter, FileFormatDataWriter, SingleDirectoryDataWriter, WriteFilesSpec, WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

import org.apache.comet.shims.ShimCometWriteFilesExec

/**
 * A drop-in replacement for Spark's `WriteFilesExec` that drives Spark's own `OutputWriter` from
 * Arrow batches, without ever materializing an `UnsafeRow`.
 *
 * Spark's write path is typed on `InternalRow` throughout - `OutputWriter.write`,
 * `FileFormatDataWriter.write` and `WriteTaskStatsTracker.newRow` - and the writers read each row
 * and encode it before asking for the next one. `ColumnarBatch.rowIterator()` already produces a
 * reused `ColumnarBatchRow` that is a zero-copy view over the Arrow buffers and satisfies that
 * contract exactly, so the `UnsafeProjection` performed by [[CometColumnarToRowExec]] is a copy
 * the writer only undoes again.
 *
 * A partitioned or bucketed write pays for that copy twice. `BaseDynamicPartitionDataWriter`
 * projects every row a second time through `getOutputRow` purely to strip the partition and
 * bucket columns before handing it to the `OutputWriter`. This node replaces that projection with
 * a pruned view over the same Arrow vectors, which costs nothing:
 * [[CometRowViewDynamicPartitionWriter]] overrides only `writeRecord`, so Spark keeps ownership
 * of partition-change detection, file rolling and `maxRecordsPerFile`.
 *
 * Everything above the per-task write stays with Spark. Because this node extends
 * `WriteFilesExecBase` (see [[ShimCometWriteFilesExec]]), `V1WritesUtils.getWriteFilesOpt` finds
 * it and `InsertIntoHadoopFsRelationCommand` / `FileFormatWriter` continue to own SaveMode
 * semantics, the commit protocol, `_SUCCESS`, dynamic partition overwrite, stats tracker
 * aggregation and catalog updates. Extending the trait is also what keeps AQE from re-inserting a
 * second `WriteFilesExec` above this node.
 *
 * Unlike [[CometWriteFilesExec]]-style native writes, the bytes are still produced by Spark's own
 * `OutputWriter`, so output is Spark's by construction and every `FileFormat` that Spark ships is
 * supported.
 *
 * Only [[org.apache.comet.rules.EliminateRedundantTransitions]] introduces this node, and only
 * once it has established the preconditions the reused row depends on. See `writeRowViewEligible`
 * there.
 *
 * @param child
 *   The Comet columnar operator producing the batches to write. Its output must be the write's
 *   `allColumns`, in order.
 */
case class CometRowViewWriteFilesExec(child: SparkPlan)
    extends ShimCometWriteFilesExec
    with CometPlan {

  override def nodeName: String = "CometRowViewWriteFiles"

  /** Spark drives this node through `executeWrite`, never `execute`. */
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(s"$nodeName does not support doExecute")

  override protected def doExecuteWrite(
      writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    // Same identifier scheme as FileFormatWriter, so committers that parse the job ID agree.
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    val dataOrdinals = CometRowViewWriteFilesExec.dataColumnOrdinals(description)

    val childRDD = child.executeColumnar()

    // SPARK-23271: a zero-partition input would spawn no task and therefore write no file at all,
    // leaving the output directory without a schema for readers. Spark's own WriteFilesExec swaps
    // in a dummy single-partition RDD for exactly this case.
    val writeRDD = if (childRDD.getNumPartitions == 0) {
      sparkContext.parallelize(Seq.empty[ColumnarBatch], 1)
    } else {
      childRDD
    }

    // Everything the task needs is resolved on the driver and captured by value. The closure must
    // not touch `this`, which holds the whole converted child subtree; Spark's own
    // WriteFilesExec.doExecuteWrite avoids the same capture by delegating to a static
    // FileFormatWriter.executeTask.
    writeRDD.mapPartitionsInternal { batches =>
      val taskCtx = TaskContext.get()
      Iterator(
        CometRowViewWriteFilesExec.executeTask(
          description,
          jobTrackerID,
          taskCtx.stageId(),
          taskCtx.partitionId(),
          // Truncation to Int matches FileFormatWriter: the masked low bits are what the Hadoop
          // TaskAttemptID accepts, and uniqueness within a job is preserved by the task ID.
          taskCtx.taskAttemptId().toInt & Integer.MAX_VALUE,
          committer,
          batches,
          dataOrdinals))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object CometRowViewWriteFilesExec extends Logging {

  /**
   * Positions of the write's data columns within its `allColumns`, used to prune the partition
   * and bucket columns off an Arrow batch without copying.
   */
  private[comet] def dataColumnOrdinals(description: WriteJobDescription): Array[Int] = {
    val positions = description.allColumns.map(_.exprId).zipWithIndex.toMap
    description.dataColumns.map { attr =>
      positions.getOrElse(
        attr.exprId,
        throw new IllegalStateException(
          s"Data column ${attr.name} is not among the write's output columns"))
    }.toArray
  }

  /**
   * Write one task's batches and commit or abort it. A direct port of
   * `FileFormatWriter.executeTask`, differing only in the writer chosen for the partitioned and
   * bucketed case and in reading rows from Arrow batches rather than from an `Iterator` of
   * `UnsafeRow`.
   *
   * `DynamicPartitionDataConcurrentWriter` is deliberately never constructed here: it spills
   * through `UnsafeKVExternalSorter`, which is typed on `UnsafeRow`. The rule refuses the rewrite
   * unless `spark.sql.maxConcurrentOutputFileWriters` is 0, which is also what makes `V1Writes`
   * plant the sort that `DynamicPartitionDataSingleWriter` requires.
   */
  private[comet] def executeTask(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      batches: Iterator[ColumnarBatch],
      dataOrdinals: Array[Int]): WriteTaskResult = {

    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    val taskAttemptContext: TaskAttemptContext = {
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)
      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    var dataWriter: FileFormatDataWriter = null

    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      dataWriter = if (sparkPartitionId != 0 && !batches.hasNext) {
        // In case of empty job, leave first partition to save meta for file format like parquet.
        new EmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        // SingleDirectoryDataWriter hands the row straight to the OutputWriter, so the batch row
        // needs no pruning and Spark's own writer can be used unchanged.
        new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else {
        new CometRowViewDynamicPartitionWriter(description, taskAttemptContext, committer)
      }

      writeBatches(dataWriter, batches, dataOrdinals)
      dataWriter.commit()
    })(
      catchBlock = {
        if (dataWriter != null) {
          dataWriter.abort()
        } else {
          committer.abortTask(taskAttemptContext)
        }
        logError(s"Job: $jobId, Task: $taskId, Task attempt $taskAttemptId aborted.")
      },
      finallyBlock = {
        if (dataWriter != null) {
          dataWriter.close()
        }
      })
  }

  /**
   * Feed a task's batches to the writer one row at a time, mirroring
   * `FileFormatDataWriter.writeWithIterator`.
   *
   * For the partitioned and bucketed case two row iterators are advanced in lockstep over the
   * same batch: one over all columns, which Spark's writer uses to detect partition and bucket
   * changes, and one over the pruned batch, which is what reaches the `OutputWriter`. Both are
   * views over the same Arrow vectors, so the pair costs one object per batch rather than a copy
   * per row.
   */
  private def writeBatches(
      dataWriter: FileFormatDataWriter,
      batches: Iterator[ColumnarBatch],
      dataOrdinals: Array[Int]): Unit = {
    var count = 0L
    dataWriter match {
      case rowViewWriter: CometRowViewDynamicPartitionWriter =>
        while (batches.hasNext) {
          val batch = batches.next()
          val allRows = batch.rowIterator()
          val dataRows = prune(batch, dataOrdinals).rowIterator()
          while (allRows.hasNext) {
            rowViewWriter.setDataRow(dataRows.next())
            rowViewWriter.writeWithMetrics(allRows.next(), count)
            count += 1
          }
        }
      case _ =>
        while (batches.hasNext) {
          val rows = batches.next().rowIterator()
          while (rows.hasNext) {
            dataWriter.writeWithMetrics(rows.next(), count)
            count += 1
          }
        }
    }
  }

  /**
   * A batch over the data columns only, borrowing the vectors of `batch`. Never closed: the
   * vectors belong to `batch`, which the child's iterator owns.
   */
  private def prune(batch: ColumnarBatch, dataOrdinals: Array[Int]): ColumnarBatch = {
    val vectors = new Array[ColumnVector](dataOrdinals.length)
    var i = 0
    while (i < dataOrdinals.length) {
      vectors(i) = batch.column(dataOrdinals(i))
      i += 1
    }
    new ColumnarBatch(vectors, batch.numRows())
  }
}

/**
 * `DynamicPartitionDataSingleWriter` with its per-row `getOutputRow` projection replaced by a
 * pruned view over the Arrow batch.
 *
 * `BaseDynamicPartitionDataWriter.writeRecord` projects every row through
 * `UnsafeProjection.create(dataColumns, allColumns)` for the sole purpose of dropping the
 * partition and bucket columns. On a columnar batch that pruning is free, so the only thing this
 * subclass changes is where the output row comes from. Partition-change detection, writer renewal
 * and `maxRecordsPerFile` are all inherited unchanged, and still see the full row.
 *
 * The caller must call [[setDataRow]] immediately before each `writeWithMetrics`. The field is
 * cleared on use so that a missed call fails loudly instead of silently rewriting the previous
 * row.
 */
private[comet] class CometRowViewDynamicPartitionWriter(
    description: WriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol)
    extends DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer) {

  private var dataRow: InternalRow = _

  def setDataRow(row: InternalRow): Unit = {
    dataRow = row
  }

  override protected def writeRecord(record: InternalRow): Unit = {
    val outputRow = dataRow
    if (outputRow == null) {
      throw new IllegalStateException("setDataRow must be called before each write")
    }
    dataRow = null
    currentWriter.write(outputRow)
    statsTrackers.foreach(_.newRow(currentWriter.path(), outputRow))
    recordsInFile += 1
  }
}

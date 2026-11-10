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

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The read tasks one Spark task of a rewrite reads: a bin of the planner, which is about one
 * output file of data. Comet's native Iceberg scan reads the `FileScanTask`s of an input
 * partition through a `taskGroup()` that has a `tasks()` method, which is this.
 */
final class BinTasks(val tasks: java.util.List[AnyRef]) extends Serializable

/**
 * What a rewrite job reads: the table, and the bins of its file groups, one per Spark partition.
 *
 * @param table
 *   the Iceberg `Table`
 * @param icebergSchema
 *   the Iceberg `Schema` the scan reads, which is the table's
 * @param sparkSchema
 *   the same schema as Spark's `StructType`
 * @param bins
 *   the read tasks of each Spark partition
 */
final class CompactionBatch(
    val table: AnyRef,
    val icebergSchema: AnyRef,
    val sparkSchema: StructType,
    val bins: Seq[BinTasks]) {

  /** Every task of every bin, for the scan accessor Comet reads the tasks from. */
  val allTasks: java.util.List[AnyRef] = {
    val all = new java.util.ArrayList[AnyRef]()
    bins.foreach(bin => all.addAll(bin.tasks))
    all
  }
}

/**
 * The batches a driver has registered for [[CompactionSource]] to read. Iceberg keeps the tasks
 * of a staged rewrite the same way, in a registry its reader looks them up in by id.
 */
object CompactionBatches {

  private val batches = new ConcurrentHashMap[String, CompactionBatch]()

  def register(id: String, batch: CompactionBatch): Unit = {
    batches.put(id, batch)
    ()
  }

  def get(id: String): Option[CompactionBatch] = Option(batches.get(id))

  def remove(id: String): Unit = {
    batches.remove(id)
    ()
  }
}

/**
 * A Spark data source that reads a [[CompactionBatch]], given the id it was registered under as
 * the `batch-id` option.
 *
 * Iceberg reads a staged rewrite through `SparkStagedScan`, which packs the tasks into Spark
 * partitions itself, without regard for the file group a task came from. A rewrite that runs
 * every group of a commit in one job needs the partitions to be the bins the planner chose, so
 * each stays within one group and one partition of the table. This source gives it that, and
 * nothing else: it only describes the scan, and Comet's native Iceberg scan reads it. The scan is
 * never read by Spark, so the caller checks that Comet takes it over before running anything.
 */
class CompactionSource extends TableProvider {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    batchOf(options).sparkSchema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table =
    new CompactionTable(batchOf(new CaseInsensitiveStringMap(properties)))

  override def supportsExternalMetadata(): Boolean = false

  private def batchOf(options: CaseInsensitiveStringMap): CompactionBatch = {
    val id = options.get(CompactionSource.BatchIdOption)
    CompactionBatches
      .get(id)
      .getOrElse(throw new IllegalStateException(s"No compaction batch is registered as '$id'"))
  }
}

object CompactionSource {

  /** The option that names the registered [[CompactionBatch]]. */
  val BatchIdOption = "batch-id"
}

private[compaction] final class CompactionTable(batch: CompactionBatch)
    extends Table
    with SupportsRead {

  override def name(): String = "comet_iceberg_compaction"

  override def schema(): StructType = batch.sparkSchema

  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ScanBuilder {
      override def build(): Scan = new CompactionScan(batch)
    }
}

/**
 * The scan of a [[CompactionBatch]]. Comet recognizes it by its class name in `CometScanRule` and
 * reads the table, schema and tasks off the accessors below, the same ones it reads off Iceberg's
 * own scans.
 */
class CompactionScan(private val batch: CompactionBatch) extends Scan with Batch {

  override def readSchema(): StructType = batch.sparkSchema

  override def description(): String =
    s"CometIcebergCompactionScan(bins=${batch.bins.size}, tasks=${batch.allTasks.size()})"

  override def toBatch: Batch = this

  private val partitions: Array[InputPartition] =
    batch.bins.map(bin => new CompactionInputPartition(bin): InputPartition).toArray

  override def planInputPartitions(): Array[InputPartition] = partitions

  override def createReaderFactory(): PartitionReaderFactory = CompactionReaderFactory

  /** The Iceberg `Table`, read reflectively by Comet. */
  def table(): AnyRef = batch.table

  /** The Iceberg `Schema` the scan reads, read reflectively by Comet. */
  def expectedSchema(): AnyRef = batch.icebergSchema

  /** Every task of the scan, read reflectively by Comet. */
  def tasks(): java.util.List[AnyRef] = batch.allTasks

  override def equals(other: Any): Boolean = other match {
    case that: CompactionScan => this.batch eq that.batch
    case _ => false
  }

  override def hashCode(): Int = System.identityHashCode(batch)
}

/**
 * One Spark partition of a [[CompactionScan]]. Comet's native Iceberg scan reads its tasks
 * through `taskGroup()`, the name Iceberg's own input partitions give theirs.
 */
final class CompactionInputPartition(val taskGroup: BinTasks) extends InputPartition

/** Spark must never read a [[CompactionScan]] itself, because only Comet's native scan can. */
private object CompactionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException(
      "A compaction scan can only be read by Comet's native Iceberg scan")
}

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

package org.apache.comet.local

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnaryExecNode}

/**
 * Row result boundary of a local query. Spark's collect encodes and compresses every row of each
 * result partition in its task, then decodes them on the driver. A local collect instead runs the
 * whole query as one native graph in a single Spark task (keeping cancellation, job groups and
 * SQL metrics), which hands copied rows to the driver in this JVM, since admission requires an
 * in-process local master.
 *
 * Only `collect` runs that way. Everything else reads the result as an RDD: a take requests
 * partitions in growing batches, an iterator fetches one partition at a time, a tail scans from
 * the last one, and a cache, `df.rdd` or PySpark's collect keep its partitions. So the node
 * executes as the ordinary native block otherwise, one Spark task per partition, and reports the
 * block's partitioning and ordering.
 */
case class CometLocalResultExec(child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = ordinary.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = ordinary.outputOrdering

  override protected def doExecute(): RDD[InternalRow] = ordinary.execute()

  override def executeCollect(): Array[InternalRow] =
    if (sparkContext.isLocal) LocalResultHandoff.collect(child.execute(), maxResultSize)
    else super.executeCollect()

  private def ordinary: SparkPlan =
    child
      .collectFirst { case local: CometLocalExec => ColumnarToRowExec(local.block) }
      .getOrElse(child)

  // Spark enforces this on serialized task results; this boundary enforces it while copying.
  private def maxResultSize: Long =
    sparkContext.getConf.getSizeAsBytes("spark.driver.maxResultSize", "1g")

  override protected def withNewChildInternal(newChild: SparkPlan): CometLocalResultExec =
    copy(child = newChild)
}

/** Driver-owned result slots, keyed by numeric IDs that the task closure carries. */
private[local] object LocalResultHandoff {
  private final class Slot(val maxResultSize: Long) {
    @volatile var rows: Array[InternalRow] = _
    @volatile var exceededBytes: Long = -1L
  }

  private val slots = new ConcurrentHashMap[Long, Slot]()
  private val nextId = new AtomicLong()

  /** Number of live slots; zero whenever no collect is running. */
  def active: Int = slots.size()

  /**
   * Runs the single result partition of `rdd` and returns its rows. Stops reading, which closes
   * the native query, once the copied rows exceed `maxResultSize` bytes (unlimited if not
   * positive). Unlike Spark's compressed size, this counts uncompressed UnsafeRow bytes, so it is
   * the more conservative of the two.
   */
  def collect(rdd: RDD[InternalRow], maxResultSize: Long): Array[InternalRow] = {
    require(rdd.getNumPartitions == 1, "Local results have exactly one partition")
    val id = nextId.incrementAndGet()
    val slot = new Slot(maxResultSize)
    slots.put(id, slot)
    try {
      rdd.sparkContext.runJob(rdd, (rows: Iterator[InternalRow]) => fill(id, rows), Seq(0))
      if (slot.exceededBytes >= 0) {
        throw new SparkException(
          s"Total size of local query results (at least ${slot.exceededBytes} bytes, " +
            s"uncompressed) is bigger than spark.driver.maxResultSize ($maxResultSize bytes)")
      }
      Option(slot.rows).getOrElse(throw new IllegalStateException("Local result not delivered"))
    } finally {
      slots.remove(id)
      ()
    }
  }

  // Runs in the result task. A retried attempt rebuilds the result and publishes it only once
  // complete. A slot the driver already removed means nobody will read the rows.
  private def fill(id: Long, rows: Iterator[InternalRow]): Unit = {
    val slot = slots.get(id)
    if (slot == null) return
    val buffer = new ArrayBuffer[InternalRow]()
    var bytes = 0L
    while (rows.hasNext) {
      // ColumnarToRow reuses one UnsafeRow; Spark's collect makes the same assumption.
      val row = rows.next().asInstanceOf[UnsafeRow].copy()
      bytes += row.getSizeInBytes
      if (slot.maxResultSize > 0 && bytes > slot.maxResultSize) {
        slot.exceededBytes = bytes
        return
      }
      buffer += row
    }
    slot.rows = buffer.toArray
  }
}

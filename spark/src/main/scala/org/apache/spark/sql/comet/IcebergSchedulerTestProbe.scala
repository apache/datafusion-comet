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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Internal, explicitly scoped test instrumentation; no SQLConf or production configuration. */
private[apache] trait IcebergSchedulerTestProbe extends Serializable {
  // Before constructing the parent iterator, including any eager shuffle block fetches.
  def beforeInput(): Unit
  // Invoked before pulling another batch. Rows count supplied input, including prefetch.
  def afterInput(rows: Long, dataLocation: String, operationId: String): Unit
  def afterHandoff(locations: Seq[String]): Unit
  // Invoked on the driver for messages actually accepted by Spark's runJob result handler.
  def accepted(partition: Int, locations: Seq[String]): Unit
  def committed(): Unit
}

private[apache] object IcebergSchedulerTestProbe {
  private val installed = new AtomicReference[IcebergSchedulerTestProbe]()

  def current: Option[IcebergSchedulerTestProbe] = Option(installed.get())

  def withProbe[T](probe: IcebergSchedulerTestProbe)(body: => T): T = {
    require(installed.compareAndSet(null, probe), "scheduler test probe already installed")
    try body
    finally installed.set(null)
  }
}

/**
 * Gate before parent.iterator: a mapPartitions callback would run after eager shuffle fetches.
 */
private[comet] class IcebergSchedulerProbeRDD(
    parent: RDD[ColumnarBatch],
    probe: IcebergSchedulerTestProbe,
    dataLocation: String,
    operationId: String)
    extends RDD[ColumnarBatch](parent) {
  override protected def getPartitions: Array[Partition] =
    firstParent[ColumnarBatch].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    probe.beforeInput()
    val input = firstParent[ColumnarBatch].iterator(split, context)
    new Iterator[ColumnarBatch] {
      private var rows = 0L
      private var pending = false

      private def observe(): Unit = {
        if (pending) {
          pending = false
          probe.afterInput(rows, dataLocation, operationId)
        }
      }

      override def hasNext: Boolean = {
        observe()
        input.hasNext
      }

      override def next(): ColumnarBatch = {
        observe()
        val batch = input.next()
        rows += batch.numRows().toLong
        pending = true
        batch
      }
    }
  }
}

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

/*
 * Aligns a lazy broadcast input with each probe partition. The driver captures the schema and
 * memory cap; executor compute passes on Spark's actual Broadcast handle without reading its
 * payload. Without a reuse owner, CometExecRDD opens an ordinary Arrow stream. With an owner,
 * native execution chooses whether to reuse a build or open a stream for this task.
 */

import org.apache.spark.{CometBroadcastMemoryManager, OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Broadcast input slots that preserve the actual Broadcast handle without constructing a decoder.
 * The schema and memory cap are captured on the driver; executor SQLConf cannot alter admission.
 */
private[comet] class CometBroadcastInputRDD(
    batches: CometBatchRDD,
    schema: StructType,
    maxBytes: Long,
    name: String)
    extends RDD[CometBroadcastInput](batches.context, Seq(new OneToOneDependency(batches))) {

  /** Reuse the parent's aligned broadcast partitions without evaluating their payloads. */
  override protected def getPartitions: Array[Partition] =
    firstParent[ColumnarBatch].partitions

  /**
   * Construct one lazy marker and its early cleanup listener; this never invokes Broadcast.value.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[CometBroadcastInput] = {
    val partition = split.asInstanceOf[CometBatchPartition]
    Iterator.single(
      new CometBroadcastInput(
        partition.value,
        schema,
        name,
        context,
        CometBroadcastMemoryManager.getOrCreate(maxBytes)))
  }
}

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

import org.apache.spark.{CometBroadcastMemoryManager, OneToOneDependency, Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Align broadcast handles with probe partitions without computing the parent's decoded batches.
 * The schema and memory cap are captured on the driver; executor SQLConf cannot alter admission.
 */
private[comet] class CometBroadcastInputRDD(
    batches: CometBatchRDD,
    schema: StructType,
    maxBytes: Long,
    name: String)
    extends RDD[CometBroadcastInput](batches.context, Seq(new OneToOneDependency(batches))) {

  override protected def getPartitions: Array[Partition] =
    firstParent[ColumnarBatch].partitions

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

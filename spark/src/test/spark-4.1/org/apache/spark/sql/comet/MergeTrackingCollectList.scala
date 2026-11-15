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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectList
import org.apache.spark.util.LongAccumulator

// Track values copied by executor-side merges without changing collect_list's aggregation.
private[comet] class MergeTrackingCollectList(
    child: Expression,
    mutableOffset: Int,
    inputOffset: Int,
    executorMergedValues: LongAccumulator)
    extends CollectList(child, mutableOffset, inputOffset) {

  override def merge(buffer: ArrayBuffer[Any], other: ArrayBuffer[Any]): ArrayBuffer[Any] = {
    if (TaskContext.get() != null) {
      executorMergedValues.add(other.size.toLong)
    }
    super.merge(buffer, other)
  }

  override def otherCopyArgs: Seq[AnyRef] = Seq(executorMergedValues)

  override def copy(
      child: Expression = this.child,
      mutableAggBufferOffset: Int = this.mutableAggBufferOffset,
      inputAggBufferOffset: Int = this.inputAggBufferOffset): CollectList =
    new MergeTrackingCollectList(
      child,
      mutableAggBufferOffset,
      inputAggBufferOffset,
      executorMergedValues)
}

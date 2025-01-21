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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A RDD that executes Spark SQL query in Comet native execution to generate ColumnarBatch.
 */
private[spark] class CometExecRDD(
    sc: SparkContext,
    partitionNum: Int,
    var f: (Seq[Iterator[ColumnarBatch]], Int, Int) => Iterator[ColumnarBatch])
    extends RDD[ColumnarBatch](sc, Nil) {

  override def compute(s: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    f(Seq.empty, partitionNum, s.index)
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(partitionNum)(i =>
      new Partition {
        override def index: Int = i
      })
  }
}

object CometExecRDD {
  def apply(sc: SparkContext, partitionNum: Int)(
      f: (Seq[Iterator[ColumnarBatch]], Int, Int) => Iterator[ColumnarBatch])
      : RDD[ColumnarBatch] =
    withScope(sc) {
      new CometExecRDD(sc, partitionNum, f)
    }

  private[spark] def withScope[U](sc: SparkContext)(body: => U): U =
    RDDOperationScope.withScope[U](sc)(body)
}

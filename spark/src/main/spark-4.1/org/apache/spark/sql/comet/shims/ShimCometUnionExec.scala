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

package org.apache.spark.sql.comet.shims

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, SQLPartitioningAwareUnionRDD}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}

object ShimCometUnionExec {

  /**
   * Unions a sequence of RDDs while preserving the declared output partitioning. Spark 4.1
   * introduced [[org.apache.spark.sql.internal.SQLConf.UNION_OUTPUT_PARTITIONING]] (SPARK-52921),
   * which lets [[org.apache.spark.sql.execution.UnionExec]] report a non-trivial output
   * partitioning when all children share the same partitioning. Downstream operators may then
   * skip an otherwise-required shuffle, so the columnar Union path must honor that contract by
   * routing through [[SQLPartitioningAwareUnionRDD]] rather than plain `SparkContext.union`,
   * which concatenates partitions and breaks the partitioning invariant.
   */
  def unionRDDs[T: ClassTag](
      sc: SparkContext,
      rdds: Seq[RDD[T]],
      outputPartitioning: Partitioning): RDD[T] = {
    outputPartitioning match {
      case _: UnknownPartitioning => sc.union(rdds)
      case _ =>
        val numPartitions = outputPartitioning.numPartitions
        val nonEmpty = rdds.filter(_.partitions.nonEmpty)
        // SQLPartitioningAwareUnionRDD indexes every child at every output partition, so any
        // child whose partition count diverges from the declared numPartitions would raise
        // ArrayIndexOutOfBoundsException. That would only happen if the declared partitioning
        // is stale relative to the RDDs (e.g. children were coalesced by AQE but the reported
        // partitioning was not). Fall back to plain concat in that case.
        if (nonEmpty.isEmpty || nonEmpty.exists(_.partitions.length != numPartitions)) {
          sc.union(rdds)
        } else {
          new SQLPartitioningAwareUnionRDD(sc, nonEmpty, numPartitions)
        }
    }
  }
}

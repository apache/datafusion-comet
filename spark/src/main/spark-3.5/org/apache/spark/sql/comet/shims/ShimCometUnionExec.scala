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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

object ShimCometUnionExec {

  /**
   * Unions a sequence of RDDs while preserving the declared output partitioning. Before Spark
   * 4.1, [[org.apache.spark.sql.execution.UnionExec]] always reports
   * [[org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning]], so this shim simply
   * concatenates partitions via `SparkContext.union`. The partitioning-aware path is only needed
   * on Spark 4.1+ (see SPARK-52921).
   */
  def unionRDDs[T: ClassTag](
      sc: SparkContext,
      rdds: Seq[RDD[T]],
      @annotation.nowarn("cat=unused") outputPartitioning: Partitioning): RDD[T] = {
    sc.union(rdds)
  }
}

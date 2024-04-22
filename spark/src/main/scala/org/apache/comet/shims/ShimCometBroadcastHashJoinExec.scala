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

package org.apache.comet.shims

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

trait ShimCometBroadcastHashJoinExec {

  /**
   * Returns the expressions that are used for hash partitioning including `HashPartitioning` and
   * `CoalescedHashPartitioning`. They shares same trait `HashPartitioningLike` since Spark 3.4,
   * but Spark 3.2/3.3 doesn't have `HashPartitioningLike` and `CoalescedHashPartitioning`.
   *
   * TODO: remove after dropping Spark 3.2 and 3.3 support.
   */
  def getHashPartitioningLikeExpressions(partitioning: Partitioning): Seq[Expression] = {
    partitioning.getClass.getDeclaredMethods
      .filter(_.getName == "expressions")
      .flatMap(_.invoke(partitioning).asInstanceOf[Seq[Expression]])
  }
}

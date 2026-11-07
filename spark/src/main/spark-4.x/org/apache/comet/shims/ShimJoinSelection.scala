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
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftSingle}

trait ShimJoinSelection extends JoinSelectionHelper {

  /** False for keys whose collation is not binary stable, which Spark never hash joins. */
  protected def hashJoinSupportedShim(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Boolean = hashJoinSupported(leftKeys, rightKeys)

  /** Spark plans a LeftSingle join as a hash or nested loop join, never a sort-merge join. */
  protected def canSortMergeJoin(joinType: JoinType): Boolean = joinType != LeftSingle
}

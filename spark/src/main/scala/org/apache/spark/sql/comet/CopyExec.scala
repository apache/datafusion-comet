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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

case class CopyExec(override val child: SparkPlan, copyMode: CopyMode) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    // This method should never be invoked as CopyExec is an internal operator used
    // during native execution offload to handle data deep copying/cloning Record batches
    // The actual execution happens in the native layer through CometExecNode.
    throw new UnsupportedOperationException(
      "This method should not be called directly - CopyExec operator is meant for internal purposes only")
  }
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)
}

sealed abstract class CopyMode {}
case object UnpackOrDeepCopy extends CopyMode
case object UnpackOrClone extends CopyMode

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

import scala.reflect.ClassTag

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.comet.execution.arrow.CometNativeArrowSource
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometSink

/**
 * An empty native input. Spark's eliminated logical subtree is explanation data only; neither it
 * nor an Arrow reader needs to run. Preserve the zero partitions of EmptyRelationExec so Spark's
 * exchanges continue to control aggregate and join partitioning.
 */
case class CometEmptyRelationExec(originalPlan: SparkPlan, override val output: Seq[Attribute])
    extends CometExec
    with LeafExecNode
    with CometNativeArrowSource {

  // Render Spark's preserved logical subtree without adding an executable child.
  override def innerChildren: Seq[QueryPlan[_]] = Seq(originalPlan)

  override protected def mapToReaders[T: ClassTag](
      consume: (String, BufferAllocator => ArrowReader) => Iterator[T]): RDD[T] =
    sparkContext.emptyRDD[T]

  override def doCanonicalize(): SparkPlan = {
    val canonical = originalPlan.canonicalized
    copy(originalPlan = canonical, output = canonical.output)
  }
}

object CometEmptyRelationExec extends CometSink[SparkPlan] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_EMPTY_RELATION_ENABLED)

  override def createExec(nativeOp: Operator, op: SparkPlan): CometNativeExec =
    CometScanWrapper(nativeOp, CometEmptyRelationExec(op, op.output))
}

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

import org.apache.spark.api.python.{PythonAccumulatorV2, PythonBroadcast, PythonEvalType, PythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, PythonUDF}
import org.apache.spark.sql.execution.{ColumnarToRowExec, LeafExecNode}
import org.apache.spark.sql.execution.python.MapInArrowExec
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf
import org.apache.comet.rules.EliminateRedundantTransitions

/** Minimal CometPlan leaf used to anchor the rule's transform without triggering execution. */
private case class StubCometLeaf(override val output: Seq[Attribute])
    extends LeafExecNode
    with CometPlan {
  override def supportsColumnar: Boolean = true
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException
}

/**
 * Plan-rule test for the `EliminateRedundantTransitions` rewrite that produces
 * `CometMapInBatchExec`. Pure Python execution paths are covered by the pytest module
 * `test_pyarrow_udf.py`; this suite verifies the JVM-side rule without spinning up Python.
 *
 * Lives under `org.apache.spark.sql.comet` so it can reference Spark's `private[spark]`
 * `PythonFunction` / `PythonAccumulatorV2` / `PythonBroadcast` classes when fabricating a stub
 * `PythonUDF` for `MapInArrowExec` to wrap.
 */
class CometMapInBatchSuite extends CometTestBase {

  private def stubPythonUDF: PythonUDF = {
    val pyFunc = new PythonFunction {
      override val command: Seq[Byte] = Seq.empty[Byte]
      override val envVars: java.util.Map[String, String] =
        new java.util.HashMap[String, String]()
      override val pythonIncludes: java.util.List[String] =
        java.util.Collections.emptyList[String]()
      override val pythonExec: String = "python3"
      override val pythonVer: String = "3"
      override val broadcastVars: java.util.List[Broadcast[PythonBroadcast]] =
        java.util.Collections.emptyList[Broadcast[PythonBroadcast]]()
      override val accumulator: PythonAccumulatorV2 = null
    }
    PythonUDF(
      name = "test_udf",
      func = pyFunc,
      dataType = StructType(Seq(StructField("id", LongType))),
      children = Seq(AttributeReference("id", LongType)(ExprId(0L))),
      evalType = PythonEvalType.SQL_MAP_ARROW_ITER_UDF,
      udfDeterministic = true)
  }

  private def buildPlan(): MapInArrowExec = {
    val cometChild = StubCometLeaf(Seq(AttributeReference("id", LongType)(ExprId(0L))))
    MapInArrowExec(
      stubPythonUDF,
      cometChild.output,
      ColumnarToRowExec(cometChild),
      isBarrier = false,
      profile = None)
  }

  test("rule rewrites MapInArrowExec over Comet to CometMapInBatchExec") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val rewritten = EliminateRedundantTransitions(spark).apply(buildPlan())
      assert(
        rewritten.exists(_.isInstanceOf[CometMapInBatchExec]),
        s"expected CometMapInBatchExec in rewritten plan:\n$rewritten")
    }
  }

  test("rule does not rewrite when feature is disabled") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "false") {
      val rewritten = EliminateRedundantTransitions(spark).apply(buildPlan())
      assert(
        !rewritten.exists(_.isInstanceOf[CometMapInBatchExec]),
        s"unexpected CometMapInBatchExec when disabled:\n$rewritten")
    }
  }
}

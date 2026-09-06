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
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Expression, ExprId, Literal, PythonUDF}
import org.apache.spark.sql.execution.ColumnarToRowExec
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.rules.EliminateRedundantTransitions

/**
 * Plan-rule tests for the `EliminateRedundantTransitions` rewrite that produces
 * `CometArrowEvalPythonExec`. Python execution paths are covered by the pytest module
 * `test_scalar_python_udf.py`; this suite verifies the JVM-side rule without spinning up Python.
 *
 * Lives under `org.apache.spark.sql.comet` so it can reference Spark's `private[spark]`
 * `PythonFunction` / `PythonAccumulatorV2` / `PythonBroadcast` classes when fabricating a stub
 * `PythonUDF` for `ArrowEvalPythonExec` to wrap.
 */
class CometArrowEvalPythonSuite extends CometTestBase {

  private val inputAttr = AttributeReference("id", LongType)(ExprId(0L))
  private val resultAttr = AttributeReference("result", LongType)(ExprId(1L))

  private def stubPythonFunction: PythonFunction = new PythonFunction {
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

  private def stubPythonUDF(
      children: Seq[Expression] = Seq(inputAttr),
      evalType: Int = PythonEvalType.SQL_ARROW_BATCHED_UDF): PythonUDF =
    PythonUDF(
      name = "test_udf",
      func = stubPythonFunction,
      dataType = LongType,
      children = children,
      evalType = evalType,
      udfDeterministic = true)

  private def buildPlan(
      children: Seq[Expression] = Seq(inputAttr),
      evalType: Int = PythonEvalType.SQL_ARROW_BATCHED_UDF): ArrowEvalPythonExec = {
    val cometChild = StubCometLeaf(Seq(inputAttr))
    ArrowEvalPythonExec(
      Seq(stubPythonUDF(children, evalType)),
      Seq(resultAttr),
      ColumnarToRowExec(cometChild),
      evalType)
  }

  private def rewrite(plan: ArrowEvalPythonExec) =
    EliminateRedundantTransitions(spark).apply(plan)

  test("rule rewrites ArrowEvalPythonExec over Comet to CometArrowEvalPythonExec") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val rewritten = rewrite(buildPlan())
      assert(
        rewritten.exists(_.isInstanceOf[CometArrowEvalPythonExec]),
        s"expected CometArrowEvalPythonExec in rewritten plan:\n$rewritten")
      assert(
        rewritten.output == Seq(inputAttr, resultAttr),
        s"rewrite must preserve the operator's output:\n$rewritten")
    }
  }

  test("rewrite strips the columnar-to-row transition beneath the operator") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val rewritten = rewrite(buildPlan())
      val native = rewritten.collectFirst { case p: CometArrowEvalPythonExec => p }.get
      assert(
        native.child.isInstanceOf[StubCometLeaf],
        s"expected the Comet columnar child directly beneath the operator, got ${native.child}")
    }
  }

  test("rule rewrites every supported scalar eval type") {
    // SQL_SCALAR_ARROW_UDF is Spark 4.1+, so drive the set the shim reports rather than a
    // hard-coded list.
    val supported =
      Seq(PythonEvalType.SQL_ARROW_BATCHED_UDF, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      supported.foreach { evalType =>
        val rewritten = rewrite(buildPlan(evalType = evalType))
        assert(
          rewritten.exists(_.isInstanceOf[CometArrowEvalPythonExec]),
          s"expected eval type $evalType to be rewritten:\n$rewritten")
      }
    }
  }

  test("rule does not rewrite iterator eval types") {
    // These guarantee only that the worker returns the same total row count, not the same
    // batching, so the operator's batch pairing does not hold.
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val rewritten = rewrite(buildPlan(evalType = PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF))
      assert(
        !rewritten.exists(_.isInstanceOf[CometArrowEvalPythonExec]),
        s"unexpected rewrite of an iterator eval type:\n$rewritten")
    }
  }

  test("rule does not rewrite when a UDF argument is not an attribute of the child") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val rewritten = rewrite(buildPlan(children = Seq(Add(inputAttr, Literal(1L)))))
      assert(
        !rewritten.exists(_.isInstanceOf[CometArrowEvalPythonExec]),
        s"unexpected rewrite with a non-attribute argument:\n$rewritten")
    }
  }

  test("rule does not rewrite when a UDF argument is another Python UDF") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val rewritten = rewrite(buildPlan(children = Seq(stubPythonUDF())))
      assert(
        !rewritten.exists(_.isInstanceOf[CometArrowEvalPythonExec]),
        s"unexpected rewrite of a chained UDF:\n$rewritten")
    }
  }

  test("rule does not rewrite when useLargeVarTypes is enabled") {
    // Comet's string / binary vectors use 4-byte offsets, so the worker would not receive the
    // large_string / large_binary input types this conf requests.
    withSQLConf(
      CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true",
      SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES.key -> "true") {
      val rewritten = rewrite(buildPlan())
      assert(
        !rewritten.exists(_.isInstanceOf[CometArrowEvalPythonExec]),
        s"unexpected rewrite with useLargeVarTypes enabled:\n$rewritten")
    }
  }

  test("rule does not rewrite when the feature is disabled") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "false") {
      val rewritten = rewrite(buildPlan())
      assert(
        !rewritten.exists(_.isInstanceOf[CometArrowEvalPythonExec]),
        s"unexpected CometArrowEvalPythonExec when disabled:\n$rewritten")
    }
  }

  test("rule annotates operator with opt-in hint when the feature is disabled") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "false") {
      val rewritten = rewrite(buildPlan())
      val info = new ExtendedExplainInfo().generateExtendedInfo(rewritten)
      assert(info.contains("[COMET-INFO:"), s"expected a [COMET-INFO: hint in:\n$info")
      assert(
        info.contains(CometConf.COMET_PYARROW_UDF_ENABLED.key),
        s"expected the opt-in config key in the hint:\n$info")
    }
  }

  test("rule emits no opt-in hint when the feature is enabled") {
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val rewritten = rewrite(buildPlan())
      val info = new ExtendedExplainInfo().generateExtendedInfo(rewritten)
      assert(
        !info.contains(CometConf.COMET_PYARROW_UDF_ENABLED.key),
        s"unexpected opt-in hint when the feature is enabled:\n$info")
    }
  }

  test("rule rewrites stacked operators into stacked native operators") {
    // UDFs of different eval types cannot share an operator, so they stack. `transformUp` visits
    // the inner one first; the outer then matches against a child that is already a columnar
    // CometArrowEvalPythonExec, with no row transition to strip.
    withSQLConf(CometConf.COMET_PYARROW_UDF_ENABLED.key -> "true") {
      val cometLeaf = StubCometLeaf(Seq(inputAttr))
      val inner = ArrowEvalPythonExec(
        Seq(stubPythonUDF()),
        Seq(resultAttr),
        ColumnarToRowExec(cometLeaf),
        PythonEvalType.SQL_ARROW_BATCHED_UDF)
      val outerResult = AttributeReference("result2", LongType)(ExprId(2L))
      val outer = ArrowEvalPythonExec(
        Seq(
          stubPythonUDF(
            children = Seq(resultAttr),
            evalType = PythonEvalType.SQL_SCALAR_PANDAS_UDF)),
        Seq(outerResult),
        inner,
        PythonEvalType.SQL_SCALAR_PANDAS_UDF)

      val rewritten = EliminateRedundantTransitions(spark).apply(outer)
      val native = rewritten.collect { case p: CometArrowEvalPythonExec => p }
      assert(
        native.length == 2,
        s"expected both operators to be rewritten, got ${native.length}:\n$rewritten")
      assert(
        rewritten.output == Seq(inputAttr, resultAttr, outerResult),
        s"stacked rewrite must preserve the output:\n$rewritten")
    }
  }
}

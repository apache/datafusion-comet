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

package org.apache.spark.sql.execution

import java.io.ByteArrayOutputStream

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.{CometFilterExec, SerializedPlan}
import org.apache.spark.sql.execution.{ScalarSubquery => ExecScalarSubquery}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.types.{BinaryType, LongType}
import org.apache.spark.util.sketch.BloomFilter

import org.apache.comet.serde.OperatorOuterClass.Operator

class CometFilterDisplaySuite extends AnyFunSuite {
  private case class Input(override val output: Seq[Attribute]) extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new AssertionError("Rendering must not execute the plan")
  }

  private def filter(condition: Expression): CometFilterExec = {
    val child = Input(condition.references.toSeq)
    new CometFilterExec(
      Operator.getDefaultInstance,
      FilterExec(condition, child),
      child.output,
      condition,
      child,
      SerializedPlan(None)) {
      // Plan-info capture should be testable without a SparkContext or native execution.
      override lazy val metrics: Map[String, SQLMetric] = Map.empty
    }
  }

  private def render(plan: CometFilterExec): Seq[String] = {
    val info = SparkPlanInfo.fromSparkPlan(plan)
    Seq(
      plan.simpleString(100),
      plan.verboseStringWithOperatorId(),
      info.simpleString,
      SparkPlanGraph(info).nodes.head.makeDotNode(Map.empty))
  }

  test("UI and explain summarize large Bloom literals before formatting or evaluating them") {
    val bytes = new Array[Byte](1024 * 1024)
    val payload = new Literal(bytes, BinaryType) {
      override def toString: String =
        throw new AssertionError("Rendering must not hex-encode the Bloom payload")
      override def eval(input: InternalRow): Any =
        throw new AssertionError("Rendering must not evaluate the Bloom payload")
    }
    val key = AttributeReference("key", LongType)()
    val condition = And(IsNotNull(key), BloomFilterMightContain(payload, key))
    val plan = filter(condition)
    val nativeOp = plan.nativeOp

    render(plan).foreach { text =>
      assert(text.contains("<bloom: 1048576 bytes>"))
      assert(text.contains("might_contain"))
      assert(text.contains("key"))
      assert(text.length < 1024)
    }
    assert(plan.condition eq condition)
    assert(plan.nativeOp eq nativeOp)
    assert(plan.originalPlan.asInstanceOf[FilterExec].condition eq condition)
    assert(payload.value.asInstanceOf[Array[Byte]] eq bytes)
  }

  test("nested Bloom rendering preserves predicate evaluation and ordinary binary literals") {
    val bloom = BloomFilter.create(10L, 128L)
    bloom.putLong(7L)
    val buffer = new ByteArrayOutputStream()
    bloom.writeTo(buffer)
    val bytes = buffer.toByteArray
    val contains = BloomFilterMightContain(Literal(bytes), Literal(7L))
    val binaryEquality = EqualTo(Literal(Array[Byte](1, 2)), Literal(Array[Byte](1, 2)))
    val condition = And(contains, Or(Not(contains), binaryEquality))
    val plan = filter(condition)
    assert(condition.eval() == true)

    render(plan).foreach { text =>
      assert(text.contains(s"<bloom: ${bytes.length} bytes>"))
      assert(text.contains("0x0102"))
    }
    assert(plan.condition eq condition)
    assert(plan.condition.eval() == true)
  }

  test("empty and null Bloom literals remain distinguishable") {
    Seq(Literal(Array.emptyByteArray), Literal.create(null, BinaryType)).foreach { payload =>
      val condition = BloomFilterMightContain(payload, Literal(7L))
      val expected = if (payload.value == null) "null" else "<bloom: 0 bytes>"
      render(filter(condition)).foreach(text =>
        assert(text.contains(s"might_contain($expected,")))
      if (payload.value == null) assert(condition.eval() == null)
    }
  }

  test("rendering leaves an unexecuted Bloom scalar subquery unresolved") {
    val child = Input(Seq(AttributeReference("bloom", BinaryType)()))
    val subquery = new SubqueryExec("scalar-subquery", child) {
      override lazy val metrics: Map[String, SQLMetric] = Map.empty
    }
    val scalar = ExecScalarSubquery(subquery, NamedExpression.newExprId)
    val condition = BloomFilterMightContain(scalar, Literal(7L))
    val plan = filter(condition)

    render(plan).foreach { text =>
      assert(text.contains("scalar-subquery"))
      assert(!text.contains("<bloom:"))
    }
    assert(plan.condition eq condition)
    intercept[IllegalArgumentException](scalar.eval())
  }
}

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

package org.apache.comet

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.udf.{CometUDF, CometUDFRegistry}

/**
 * End-to-end test for [[CometUDFRegistry]]. Registering a [[CometUDF]] against a Spark UDF name
 * causes the [[org.apache.comet.serde.CometScalaUDF]] serde to route through the registered class
 * on the native path.
 */
class CometRegisteredUdfSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def afterEach(): Unit = {
    CometUDFRegistry.clear()
    super.afterEach()
  }

  test("registered CometUDF runs on the native path") {
    spark.udf.register("plus_one", (x: Int) => x + 1)
    CometUDFRegistry.register("plus_one", classOf[PlusOneCometUDF])

    withParquetTable((0 until 8).map(Tuple1(_)), "t") {
      checkSparkAnswerAndOperator(sql("SELECT plus_one(_1) FROM t"))
    }
  }

  test("unregistered ScalaUDF still falls back when codegen is disabled") {
    spark.udf.register("plus_two", (x: Int) => x + 2)
    assert(!CometUDFRegistry.isRegistered("plus_two"))

    withParquetTable((0 until 4).map(Tuple1(_)), "t") {
      checkSparkAnswer(sql("SELECT plus_two(_1) FROM t"))
    }
  }

  test("register / isRegistered / unregister round-trip") {
    assert(!CometUDFRegistry.isRegistered("plus_one"))
    CometUDFRegistry.register("plus_one", classOf[PlusOneCometUDF])
    assert(CometUDFRegistry.isRegistered("plus_one"))
    CometUDFRegistry.unregister("plus_one")
    assert(!CometUDFRegistry.isRegistered("plus_one"))
  }
}

/**
 * Test [[CometUDF]] that returns `input + 1` over an int vector. Top-level with a no-arg
 * constructor so `CometUdfBridge` can instantiate it reflectively per task.
 */
class PlusOneCometUDF extends CometUDF {
  // A RootAllocator owned by the UDF instance keeps the test self-contained. Production UDFs
  // would typically reuse a TaskContext-scoped allocator, but the per-task instance lifecycle
  // makes either choice safe.
  private val allocator = new RootAllocator()

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    val input = inputs(0).asInstanceOf[IntVector]
    val out = new IntVector("plus_one_out", allocator)
    out.allocateNew(numRows)
    var i = 0
    while (i < numRows) {
      if (input.isNull(i)) out.setNull(i)
      else out.set(i, input.get(i) + 1)
      i += 1
    }
    out.setValueCount(numRows)
    out
  }
}

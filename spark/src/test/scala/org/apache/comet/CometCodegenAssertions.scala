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

import org.apache.arrow.vector.ValueVector
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.udf.codegen.CometScalaUDFCodegen
import org.apache.comet.vector.CometVector

/**
 * Shared assertions for the codegen-dispatcher test suites. Mix in alongside `CometTestBase`.
 */
trait CometCodegenAssertions {

  /** Asserts the dispatcher actually ran during `f`, guarding against silent serde fallback. */
  protected def assertCodegenRan(f: => Unit): Unit = {
    CometScalaUDFCodegen.resetStats()
    f
    val after = CometScalaUDFCodegen.stats()
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected codegen dispatcher activity, got $after")
  }

  /**
   * Asserts the composed subtree fused into one kernel signature, not N (one per sub-expression).
   * Uses the JVM-wide signature set rather than `compileCount` because per-task `boundExpr`
   * isolation makes multi-partition queries trip `compileCount > 1` even when the bytecode is
   * shared.
   */
  protected def assertOneKernelForSubtree(f: => Unit): Unit = {
    CometScalaUDFCodegen.resetStats()
    val sigsBefore = CometScalaUDFCodegen.snapshotCompiledSignatures()
    f
    val sigsAfter = CometScalaUDFCodegen.snapshotCompiledSignatures()
    val grew = sigsAfter.size - sigsBefore.size
    assert(
      grew <= 1,
      s"expected <= 1 new compiled-kernel signature for the composed subtree, grew by $grew; " +
        s"new=${sigsAfter -- sigsBefore}")
    val after = CometScalaUDFCodegen.stats()
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected codegen dispatcher activity, got $after")
  }

  /**
   * Asserts a kernel matching the given input Arrow vector classes and output type sits in the
   * JVM-wide signature set. Pair with `assertCodegenRan` since the set is append-only. Compares
   * by simple name to be robust to Arrow shading.
   */
  protected def assertKernelSignaturePresent(
      inputs: Seq[Class[_ <: ValueVector]],
      output: DataType): Unit = {
    val sigs = CometScalaUDFCodegen.snapshotCompiledSignatures()
    val expectedNames = inputs.map(_.getSimpleName).toIndexedSeq
    val present = sigs.exists { case (cached, dt) =>
      dt == output && cached.map(_.getSimpleName) == expectedNames
    }
    assert(
      present,
      s"expected kernel signature $expectedNames -> $output; " +
        s"cache had ${sigs.map { case (c, d) => (c.map(_.getSimpleName), d) }}")
  }

  /**
   * Compiles `expr` (no input columns), runs one batch of `numRows`, and hands the output
   * `CometVector` to `read`. Every row evaluates to the same value (the expression has no input),
   * which still exercises the cross-row cumulative child index of the collection output writer:
   * the child of a List / Map grows by each row's element count, so a batch of N rows drives the
   * accumulation that a single row cannot. Drives the writer directly, without a query plan, so
   * it reaches complex-output expressions the serde does not route through dispatch today. The
   * vector is closed after `read` returns, so `read` must fully materialize what it needs.
   */
  protected def runKernel[T](expr: Expression, numRows: Int)(read: CometVector => T): T = {
    val kernel = CometBatchKernelCodegen.compile(expr, IndexedSeq.empty).newInstance()
    val field = CometBatchKernelCodegen.toFfiArrowField("out", expr.dataType, nullable = true)
    val out = CometBatchKernelCodegen.allocateOutput(field, numRows, 0)
    try {
      kernel.init(0)
      kernel.process(Array.empty[ValueVector], out, numRows)
      out.setValueCount(numRows)
      read(CometVector.getVector(out, null))
    } finally {
      out.close()
    }
  }
}

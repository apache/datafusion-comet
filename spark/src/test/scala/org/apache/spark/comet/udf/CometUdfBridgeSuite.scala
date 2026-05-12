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

package org.apache.spark.comet.udf

import java.io.File

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.spark.TaskContext

import org.apache.comet.CometArrowAllocator
import org.apache.comet.udf.{CometUDF, CometUdfBridge}

/**
 * JVM-side unit tests for `CometUdfBridge.evaluate`. Exercises the new `numRows` length contract
 * and the `TaskContext` install / unset behavior added to the bridge. The native -> JNI side of
 * the change is validated by Rust compiling against the new `JvmScalarUdfExpr::new` signature and
 * the updated `jni_sig!` for `CometUdfBridge.evaluate`; an end-to-end JNI round-trip will land
 * with the dispatcher PR that introduces production serde routing for the bridge.
 */
class CometUdfBridgeSuite extends AnyFunSuite with BeforeAndAfterAll {

  // Surefire sets `java.io.tmpdir` to `${project.build.directory}/tmp`, which Maven does not
  // auto-create. Arrow's `JniLoader` extracts `libarrow_cdata_jni.{dylib,so}` from the
  // `arrow-c-data` jar via `File.createTempFile`, which fails with `No such file or directory`
  // if the dir is missing. Other Comet suites avoid the issue because they enter Arrow C Data
  // through native code; this suite calls `Data.exportVector` directly from the JVM.
  override def beforeAll(): Unit = {
    super.beforeAll()
    new File(System.getProperty("java.io.tmpdir")).mkdirs()
  }

  private def runEvaluate(
      udfClass: String,
      numRows: Int,
      taskContext: TaskContext): ValueVector = {
    val outArr = ArrowArray.allocateNew(CometArrowAllocator)
    val outSch = ArrowSchema.allocateNew(CometArrowAllocator)
    try {
      CometUdfBridge.evaluate(
        udfClass,
        new Array[Long](0),
        new Array[Long](0),
        outArr.memoryAddress(),
        outSch.memoryAddress(),
        numRows,
        taskContext)
      Data.importVector(CometArrowAllocator, outArr, outSch, null)
    } finally {
      outArr.close()
      outSch.close()
    }
  }

  test("evaluate uses numRows as the result length contract for zero-input UDFs") {
    // Pre-numRows the bridge derived expected length from max input length, which is 0 when
    // there are no inputs, so a zero-arg UDF could not produce any rows. The fix passes
    // numRows through and uses it as the contract.
    val out = runEvaluate(classOf[RowCountTestUDF].getName, 7, null).asInstanceOf[IntVector]
    try {
      assert(out.getValueCount === 7)
      (0 until 7).foreach(i => assert(out.get(i) === 42))
    } finally {
      out.close()
    }
  }

  test("evaluate installs a propagated TaskContext when the worker thread has none") {
    val prior = TaskContext.get()
    if (prior != null) TaskContext.unset()
    try {
      val propagated = TaskContext.empty()
      RecordTaskContextUDF.reset()
      val out = runEvaluate(classOf[RecordTaskContextUDF].getName, 1, propagated)
      out.close()
      assert(
        RecordTaskContextUDF.observed === propagated,
        "bridge should install the propagated TaskContext as the thread-local for the call")
      assert(
        TaskContext.get() === null,
        "bridge must clear the thread-local in finally so Tokio workers do not leak it")
    } finally {
      if (prior != null) TaskContext.setTaskContext(prior)
    }
  }

  test("evaluate leaves the thread-local alone when no TaskContext is propagated") {
    val prior = TaskContext.get()
    if (prior != null) TaskContext.unset()
    try {
      RecordTaskContextUDF.reset()
      val out = runEvaluate(classOf[RecordTaskContextUDF].getName, 1, null)
      out.close()
      assert(
        RecordTaskContextUDF.observed === null,
        "no TaskContext propagated and none on thread, so the UDF body must observe null")
    } finally {
      if (prior != null) TaskContext.setTaskContext(prior)
    }
  }

  test("evaluate does not overwrite an existing thread-local TaskContext") {
    // Spark task threads (as opposed to Tokio workers) already have a TaskContext installed
    // by Spark; the bridge must not stomp on it with the propagated reference.
    val prior = TaskContext.get()
    val installed = TaskContext.empty()
    TaskContext.setTaskContext(installed)
    try {
      val propagated = TaskContext.empty()
      RecordTaskContextUDF.reset()
      val out = runEvaluate(classOf[RecordTaskContextUDF].getName, 1, propagated)
      out.close()
      assert(
        RecordTaskContextUDF.observed === installed,
        "current thread already has a TaskContext; bridge must leave it in place")
      assert(
        TaskContext.get() === installed,
        "thread-local must still be the originally-installed context after evaluate returns")
    } finally {
      TaskContext.unset()
      if (prior != null) TaskContext.setTaskContext(prior)
    }
  }
}

/** Zero-input UDF: returns `numRows` rows of the constant 42. */
class RowCountTestUDF extends CometUDF {
  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    val out = new IntVector("out", CometArrowAllocator)
    out.allocateNew(numRows)
    var i = 0
    while (i < numRows) {
      out.set(i, 42)
      i += 1
    }
    out.setValueCount(numRows)
    out
  }
}

object RecordTaskContextUDF {
  // Volatile because the bridge is allowed to call from any thread; the assertion thread
  // needs to observe whatever evaluate() wrote.
  @volatile var observed: TaskContext = _
  def reset(): Unit = { observed = null }
}

/** Records what `TaskContext.get()` returned at evaluate time, for assertion. */
class RecordTaskContextUDF extends CometUDF {
  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    RecordTaskContextUDF.observed = TaskContext.get()
    val out = new IntVector("out", CometArrowAllocator)
    out.allocateNew(numRows)
    var i = 0
    while (i < numRows) {
      out.set(i, 0)
      i += 1
    }
    out.setValueCount(numRows)
    out
  }
}

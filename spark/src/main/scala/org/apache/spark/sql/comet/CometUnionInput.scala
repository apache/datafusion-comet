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

import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.c.ArrowArrayStream
import org.apache.arrow.util.AutoCloseables
import org.apache.spark.{OneToOneDependency, Partition, TaskContext}
import org.apache.spark.comet.CometTaskContextShim
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.comet.execution.arrow.CometBroadcastArrowStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.Native

/** Opens the original Union iterator only after the native join has completed its build. */
class CometUnionInput private[comet] (
    iteratorFactory: () => Iterator[ColumnarBatch],
    schema: StructType,
    name: String,
    context: TaskContext,
    val nativeRootPlanIds: Array[Long])
    extends AutoCloseable {

  private val nativeLib = new Native()
  private val children = ArrayBuffer.empty[AutoCloseable]
  private var domainHandle = 0L
  private var stream: CometBroadcastArrowStream = null
  private var closed = false

  // Releasing the C stream stops pulls, but the outer iterator can still hold output buffers.
  // Keep branch resources until it closes this input after releasing those outputs.
  private val source = new Iterator[ColumnarBatch] with AutoCloseable {
    private lazy val batches = iteratorFactory()
    private var stopped = false

    override def hasNext: Boolean = !stopped && withScope(batches.hasNext)

    override def next(): ColumnarBatch = withScope(batches.next())

    override def close(): Unit = { stopped = true }
  }

  CometUnionInput.addCleanup(context)(close())

  def taskAttemptId(): Long = context.taskAttemptId()

  /** Retain the borrowed JNI handle until Arrow has finished opening and consuming branches. */
  def openStream(handle: Long): ArrowArrayStream = withScope {
    domainHandle = if (handle == 0L) 0L else nativeLib.retainUnionRuntimeFilter(handle)
    stream = CometBroadcastArrowStream.open(source, schema, name)
    stream.stream
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      val lease: AutoCloseable = () => {
        if (domainHandle != 0L) nativeLib.releaseUnionRuntimeFilter(domainHandle)
      }
      AutoCloseables.close((Seq(stream, source) ++ children.reverse :+ lease): _*)
      children.clear()
    }
  }

  // Called after the enclosing native plan and exported outputs have been released.
  def closeAfterExecution(): Unit = close()

  private def withScope[T](body: => T): T = {
    val previousTask = TaskContext.get()
    val previousInput = CometUnionInput.current.get()
    CometTaskContextShim.set(context)
    CometUnionInput.current.set(this)
    try {
      context.killTaskIfInterrupted()
      body
    } finally {
      if (previousInput == null) CometUnionInput.current.remove()
      else CometUnionInput.current.set(previousInput)
      if (previousTask == null) CometTaskContextShim.unset()
      else CometTaskContextShim.set(previousTask)
    }
  }
}

object CometUnionInput {
  // Used only during synchronous Spark iterator callbacks. Persistent ownership is on the input;
  // native forwarding carries completed domains into nested Union inputs.
  private val current = new ThreadLocal[CometUnionInput]()

  def isBranchExecution: Boolean = current.get() != null

  /** Lazy branch resources outlive the outer native plan and its exported output batches. */
  def addCleanup(context: TaskContext)(close: => Unit): Unit = {
    val input = current.get()
    if (input != null) input.children += (() => close)
    else context.addTaskCompletionListener[Unit](_ => close)
  }

  def registerExecution(plan: Long): Unit = {
    Option(current.get()).foreach { input =>
      // Native validates the exact branch root and task attempt before retaining the lease.
      if (input.domainHandle != 0L) {
        input.nativeLib.setUnionRuntimeFilter(plan, input.domainHandle)
      }
    }
  }
}

/** Preserve the original Union partitions, dependencies and preferred locations. */
private[comet] class CometUnionInputRDD(
    batches: RDD[ColumnarBatch],
    schema: StructType,
    name: String,
    nativeRootPlanIds: Array[Long])
    extends RDD[CometUnionInput](batches.context, Seq(new OneToOneDependency(batches))) {

  override val partitioner = batches.partitioner

  override protected def getPartitions: Array[Partition] =
    firstParent[ColumnarBatch].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[CometUnionInput] = {
    val parent = firstParent[ColumnarBatch]
    Iterator.single(
      new CometUnionInput(
        () => parent.iterator(split, context),
        schema,
        name,
        context,
        nativeRootPlanIds))
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[ColumnarBatch].preferredLocations(split)
}

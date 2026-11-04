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

import java.io.{ObjectInputStream, ObjectOutputStream}

/**
 * One scan's planning data for each partition. A scan can build a partition's bytes only when its
 * task is serialized, the way Spark serializes each task's input partition, so the driver never
 * holds every partition's bytes at once.
 */
private[apache] trait PartitionPlanData {
  def length: Int

  /** Partition i's bytes. Must be safe to call from any thread, and more than once. */
  def apply(i: Int): Array[Byte]

  /**
   * Whether `other` holds the same data, decided without building any partition. The default only
   * knows identity.
   */
  def sameAs(other: PartitionPlanData): Boolean = this eq other

  /** Every partition's bytes. For tests and diagnostics only: it builds them all. */
  def toArray: Array[Array[Byte]] = Array.tabulate(length)(apply)
}

private[apache] object PartitionPlanData {

  private final class Built(val bytes: Array[Array[Byte]]) extends PartitionPlanData {
    override def length: Int = bytes.length
    override def apply(i: Int): Array[Byte] = bytes(i)
    override def sameAs(other: PartitionPlanData): Boolean = other match {
      case that: Built =>
        that.bytes.length == bytes.length &&
        bytes.indices.forall(i => java.util.Arrays.equals(bytes(i), that.bytes(i)))
      case _ => false
    }
  }

  /** Data already built for every partition. */
  def apply(bytes: Array[Array[Byte]]): PartitionPlanData = new Built(bytes)

  /** Whether two scans carry the same data. */
  def same(a: PartitionPlanData, b: PartitionPlanData): Boolean = (a eq b) || a.sameAs(b)
}

/**
 * The planning data one task needs, keyed by scan. On the driver it holds only a reference to
 * every scan's data and builds this partition's bytes while the task is serialized. On the
 * executor it holds the bytes it received.
 */
private[comet] final class PlanDataSlice(
    @transient private val source: Map[String, PartitionPlanData],
    index: Int)
    extends Serializable {

  @transient private var received: Map[String, Array[Byte]] = _

  def get: Map[String, Array[Byte]] =
    if (received != null) received else source.map { case (key, data) => key -> data(index) }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    out.writeObject(get)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    received = in.readObject().asInstanceOf[Map[String, Array[Byte]]]
  }
}

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

import java.util.{LinkedHashMap => JLinkedHashMap, Map => JMap}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * What code generated over a list of columns depends on: the type and nullability of each column,
 * and the method size at which `CodegenContext.splitExpressions` splits the code for them.
 */
private[comet] case class ColumnLayout(
    columns: Seq[(DataType, Boolean)],
    methodSplitThreshold: Int)

private[comet] object ColumnLayout {

  /** The layout of `columns` under the current `spark.sql.codegen.methodSplitThreshold`. */
  def of(columns: Seq[Expression]): ColumnLayout =
    ColumnLayout(columns.map(c => (c.dataType, c.nullable)), SQLConf.get.methodSplitThreshold)
}

/**
 * Generated classes kept for the life of an executor, under a key for what their source depends
 * on.
 *
 * Spark caches a compiled class under its source, so an operator that generates code in every
 * partition still generates the source every time, which for a wide or deeply nested schema can
 * take longer than the partition's rows. Keeping the class under a cheaper key generates it once.
 * The cache holds the `maxEntries` most recently used values.
 */
private[comet] class GeneratedClassCache[K, V <: AnyRef](
    maxEntries: Int = GeneratedClassCache.MaxEntries) {

  /** Least recently used first. Guarded by `entries.synchronized`. */
  private val entries = new JLinkedHashMap[K, V](16, 0.75f, true) {
    override def removeEldestEntry(eldest: JMap.Entry[K, V]): Boolean = size() > maxEntries
  }

  private val generated = new AtomicLong(0)

  /** How many values this cache has generated, for tests. */
  def generatedCount: Long = generated.get()

  /**
   * The value for `key`, generated if absent. `generate` returns the value and whether other
   * callers may share it. It runs outside the lock, so callers that miss on the same key at once
   * each generate the value, as they would without the cache.
   */
  def getOrGenerate(key: K)(generate: => (V, Boolean)): V = {
    val cached = entries.synchronized(entries.get(key))
    if (cached != null) {
      cached
    } else {
      val (value, shareable) = generate
      generated.incrementAndGet()
      if (shareable) {
        val _ = entries.synchronized(entries.putIfAbsent(key, value))
      }
      value
    }
  }
}

private[comet] object GeneratedClassCache {

  /** The default bound of Spark's compiled class cache, `spark.sql.codegen.cache.maxEntries`. */
  val MaxEntries = 100
}

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

package org.apache.comet.udf

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.annotation.Unstable

/**
 * Driver-side registry mapping a Spark UDF name to a user-supplied [[CometUDF]] implementation
 * class. When the [[org.apache.comet.serde.CometScalaUDF]] serde encounters a `ScalaUDF` whose
 * name is registered, it routes the call through the registered class instead of the Janino
 * codegen dispatcher.
 *
 * Typical usage:
 * {{{
 *   spark.udf.register("plus_one", (x: Int) => x + 1) // row-based fallback / type info for Spark
 *   CometUDFRegistry.register("plus_one", classOf[PlusOneUdf])
 * }}}
 *
 * The matching Spark UDF must be registered separately (e.g. via `spark.udf.register`) so Spark
 * can bind the function during analysis and supply the return type. If Comet is disabled or the
 * enclosing operator falls back, Spark evaluates the row-based UDF.
 *
 * Registration is driver-side state; executors look up the class name from the serialized plan
 * and load the class via the executor's context classloader.
 */
@Unstable
object CometUDFRegistry {

  private val registry = new ConcurrentHashMap[String, Class[_ <: CometUDF]]()

  /** Register a [[CometUDF]] implementation against a Spark UDF name. */
  def register(udfName: String, udfClass: Class[_ <: CometUDF]): Unit = {
    registry.put(udfName, udfClass)
  }

  /** Remove a previously registered UDF. No-op if not registered. */
  def unregister(udfName: String): Unit = {
    registry.remove(udfName)
  }

  /** Whether a UDF name has a registered [[CometUDF]] implementation. */
  def isRegistered(udfName: String): Boolean = registry.containsKey(udfName)

  private[comet] def get(udfName: String): Option[Class[_ <: CometUDF]] =
    Option(registry.get(udfName))

  // Visible for testing.
  private[comet] def clear(): Unit = registry.clear()
}

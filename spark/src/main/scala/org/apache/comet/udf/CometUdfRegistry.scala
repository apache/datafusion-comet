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

import org.apache.spark.sql.types.DataType

/**
 * What Comet records about a UDF registered with it under a Spark function name: the signature
 * its catalog stub was installed with, and where the implementation lives.
 */
sealed trait UdfMetadata {

  /** The argument types every call must have. */
  def inputTypes: Seq[DataType]

  /** The return type Spark plans the call against. */
  def returnType: DataType
}

/**
 * A vectorized JVM UDF: a [[CometUDF]] implementation that native execution instantiates by class
 * name and calls once per batch through `CometUdfBridge`.
 */
final case class JvmUdfMetadata(
    className: String,
    inputTypes: Seq[DataType],
    returnType: DataType)
    extends UdfMetadata

/**
 * Driver-side registry of UDFs registered with Comet, keyed by Spark function name.
 * `CometScalaUDF` looks a `ScalaUDF` up here by name to recognize a call it should route to the
 * registered implementation rather than to the codegen dispatcher. Executors never consult it:
 * everything they need travels with the plan.
 *
 * This is a process-wide singleton, which the contributor guide asks be justified. The state is
 * bounded by the number of distinct names an application registers, each entry is small (a class
 * name and a signature), and it holds no credentials. The lifetime is not obviously right,
 * though: the map is keyed by bare function name with no session scoping, so two sessions sharing
 * a driver JVM share one namespace and the last registration of a name wins for both, and an
 * ordinary Scala UDF registered under a name Comet already holds is routed to Comet's entry. Both
 * are tracked: [[https://github.com/apache/datafusion-comet/issues/5294]] and
 * [[https://github.com/apache/datafusion-comet/issues/5295]].
 */
object CometUdfRegistry {

  private val byName = new ConcurrentHashMap[String, UdfMetadata]()

  /** Register or replace the entry for a name. */
  def register(name: String, meta: UdfMetadata): Unit = {
    val _ = byName.put(name, meta)
  }

  /** The entry for a name, if one is registered. */
  def get(name: String): Option[UdfMetadata] = Option(byName.get(name))
}

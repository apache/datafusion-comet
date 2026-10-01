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

/** Metadata for a registered native UDF. */
case class NativeUdfMetadata(
    libraryPath: String,
    inputTypes: Seq[DataType],
    returnType: DataType,
    deterministic: Boolean)

/**
 * Driver-side registry of native UDFs. Looked up by `QueryPlanSerde` to recognize names that
 * should be emitted as `NativeScalarUdf` instead of attempted as JVM-evaluated `ScalaUDF`s.
 */
class CometNativeUdfRegistry {
  private val byName = new ConcurrentHashMap[String, NativeUdfMetadata]()

  /** Register or replace metadata for a name. */
  def register(name: String, meta: NativeUdfMetadata): Unit =
    byName.put(name, meta)

  /** Return metadata for a name, if registered. */
  def get(name: String): Option[NativeUdfMetadata] =
    Option(byName.get(name))
}

object CometNativeUdfRegistry {

  /**
   * Process-wide singleton, which the contributor guide's "Global singletons" section asks be
   * justified here.
   *
   * The state is bounded by the number of distinct UDF names an application registers, and each
   * entry is small (a library path and a signature). It is not bounded by queries or files, and
   * there are no credentials in it to go stale.
   *
   * The lifetime is not obviously right, though, and it is an open question on this feature: the
   * map is keyed by bare function name with no session scoping, so two sessions sharing a driver
   * JVM (a Connect server, a notebook, a test JVM running several suites) share one namespace,
   * and the last registration of a name wins for all of them. A name is also matched by name
   * alone, which is why an ordinary Scala UDF registered under a name a native UDF already
   * claimed is currently answered out of the Rust library. Both are tracked:
   * [[https://github.com/apache/datafusion-comet/issues/5294]] for the scoping and
   * [[https://github.com/apache/datafusion-comet/issues/5295]] for the name collision.
   */
  private lazy val instance: CometNativeUdfRegistry = new CometNativeUdfRegistry

  /** Register or replace metadata for a name on the process-wide registry. */
  def register(name: String, meta: NativeUdfMetadata): Unit =
    instance.register(name, meta)

  /** Return metadata for a name from the process-wide registry, if registered. */
  def get(name: String): Option[NativeUdfMetadata] =
    instance.get(name)
}

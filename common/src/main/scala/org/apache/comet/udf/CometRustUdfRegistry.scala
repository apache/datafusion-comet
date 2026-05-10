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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.DataType

/** Metadata for a registered Rust UDF. */
case class RustUdfMetadata(
    libraryPath: String,
    inputTypes: Seq[DataType],
    returnType: DataType,
    deterministic: Boolean)

/**
 * Driver-side registry of Rust UDFs. Looked up by `QueryPlanSerde` to recognize names that should
 * be emitted as `RustUdfCall` instead of attempted as JVM-evaluated `ScalaUDF`s.
 */
class CometRustUdfRegistry {
  private val byName = new ConcurrentHashMap[String, RustUdfMetadata]()

  /** Register or replace metadata for a name. */
  def register(name: String, meta: RustUdfMetadata): Unit =
    byName.put(name, meta)

  /** Return metadata for a name, if registered. */
  def get(name: String): Option[RustUdfMetadata] =
    Option(byName.get(name))

  /** Snapshot the registered set as an immutable Map. */
  def snapshot: Map[String, RustUdfMetadata] =
    byName.asScala.toMap
}

object CometRustUdfRegistry {

  /** Process-wide singleton. */
  lazy val instance: CometRustUdfRegistry = new CometRustUdfRegistry
}

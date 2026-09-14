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

import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.comet.util.Utils

import com.esotericsoftware.kryo.Kryo

/**
 * Registers the classes Comet hands to Spark's serializer with Kryo.
 *
 * This is only needed when `spark.kryo.registrationRequired=true`, which makes Kryo reject any
 * unregistered class rather than writing its name. Set it alongside Comet's own configuration:
 *
 * {{{
 *   spark.serializer          org.apache.spark.serializer.KryoSerializer
 *   spark.kryo.registrator    org.apache.comet.CometKryoRegistrator
 * }}}
 *
 * `spark.kryo.registrator` has to be set before the `SparkContext` is created, because
 * `KryoSerializer` reads it when `SparkEnv` builds it. That is earlier than `CometDriverPlugin`
 * runs, so Comet cannot add this for you the way it can add `spark.sql.cache.serializer`;
 * `CometDriverPlugin` logs a warning instead when the combination looks unsafe.
 *
 * Two payloads need it: the `Array[ChunkedByteBuffer]` a native broadcast broadcasts, and
 * `CometCachedBatch`. The first applies whether or not the in-memory cache feature is enabled.
 */
class CometKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    CometKryoRegistrator.classes.foreach(kryo.register)
  }
}

object CometKryoRegistrator {
  val CLASS_NAME: String = classOf[CometKryoRegistrator].getName

  def classes: Seq[Class[_]] =
    Utils.arrowBytesKryoClasses ++ ArrowCachedBatchSerializer.kryoClasses
}

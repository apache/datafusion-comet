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

package org.apache.comet.shims

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

trait ShimCometBroadcastExchangeExec {
  // TODO: remove after dropping Spark 3.2 and 3.3 support
  protected def doBroadcast[T: ClassTag](sparkContext: SparkContext, value: T): Broadcast[Any] = {
    // Spark 3.4 has new API `broadcastInternal` to broadcast the relation without caching the
    // unserialized object.
    val classTag = implicitly[ClassTag[T]]
    val broadcasted = sparkContext.getClass.getDeclaredMethods
      .filter(_.getName == "broadcastInternal")
      .map { a => a.setAccessible(true); a }
      .map { method =>
        method
          .invoke(
            sparkContext.asInstanceOf[Object],
            value.asInstanceOf[Object],
            true.asInstanceOf[Object],
            classTag.asInstanceOf[Object])
          .asInstanceOf[Broadcast[Any]]
      }
      .headOption
    // Fallback to the old API if the new API is not available.
    broadcasted
      .getOrElse(sparkContext.broadcast(value.asInstanceOf[Object]))
      .asInstanceOf[Broadcast[Any]]
  }
}

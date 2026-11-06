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

/**
 * The JNI client a Spark driver uses to run queries on a Comet Native Executor cluster. See
 * https://github.com/apache/datafusion-comet/issues/7288.
 *
 * The native methods exist only in a libcomet built with the `native-executors` Cargo feature.
 */
class NativeExecutorsClient extends NativeBase {

  /**
   * Runs a query on the cluster and waits for it to finish.
   *
   * @param schedulerUrl
   *   the Arrow Flight URL of the cluster's scheduler
   * @param query
   *   a serialized `QueryRequest`, whose Comet version and plan format the native library fills
   *   in
   * @return
   *   a serialized `ResultEndpoints`: where each result partition is, in order
   */
  @native def submit(schedulerUrl: String, query: Array[Byte]): Array[Byte]

  /**
   * Starts reading a result partition.
   *
   * @return
   *   a handle for [[nextBatch]] and [[closePartition]]
   */
  @native def openPartition(location: String, ticket: Array[Byte]): Long

  /**
   * Moves the partition's next batch into the Arrow C Data Interface structs at `arrayAddrs` and
   * `schemaAddrs`, one pair per column.
   *
   * @return
   *   the batch's row count, or -1 at the end of the partition
   */
  @native def nextBatch(handle: Long, arrayAddrs: Array[Long], schemaAddrs: Array[Long]): Long

  /** Stops reading a partition and releases its handle. */
  @native def closePartition(handle: Long): Unit
}

object NativeExecutorsClient {

  private val missingFeature =
    "spark.comet.nativeExecutors.enabled needs a libcomet built with the `native-executors` " +
      "Cargo feature"

  /** Whether the loaded libcomet has the client's native methods, which only the feature adds. */
  private lazy val hasNativeSupport: Boolean = {
    val client = new NativeExecutorsClient()
    try {
      // A handle of 0 is a no-op once the method exists.
      client.closePartition(0)
      true
    } catch {
      case e: UnsatisfiedLinkError
          if String.valueOf(e.getMessage).contains(classOf[NativeExecutorsClient].getName) =>
        false
    }
  }

  /**
   * Fails with a clear error unless libcomet was built with the `native-executors` Cargo feature.
   * Without it, the first call into the client would fail with a bare `UnsatisfiedLinkError`.
   */
  def requireNativeSupport(): Unit = {
    if (!hasNativeSupport) {
      throw new CometRuntimeException(missingFeature)
    }
  }
}

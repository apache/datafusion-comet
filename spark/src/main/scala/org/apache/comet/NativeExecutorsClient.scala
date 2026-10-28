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

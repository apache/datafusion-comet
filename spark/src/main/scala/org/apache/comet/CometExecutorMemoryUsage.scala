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

import org.apache.spark.scheduler.SparkListenerEvent

/**
 * One sample of an executor's native memory usage log, in the form the event log records it. When
 * the application writes an event log and runs the Comet plugin, the executor sends each sample
 * to the driver plugin as well as logging it, and the driver posts a summary of them to the
 * listener bus, which writes each sample of it to the event log as JSON with `"Event"` set to
 * this class's name. See `CometExecIterator.MemoryUsageSummary`.
 *
 * The memory figures are in bytes and are the ones the log line reports; see
 * `CometExecIterator.memoryUsageMessage`.
 *
 * @param executorId
 *   The executor that took the sample, `driver` in local mode.
 * @param time
 *   When the executor took the sample, in milliseconds since the epoch by the executor's clock.
 * @param nativeAllocated
 *   The memory that Comet's native code has allocated and not yet freed, whether or not a memory
 *   pool tracks it.
 * @param poolsReserved
 *   The memory reserved across Comet's memory pools, counting a pool shared by several plans
 *   once, less any that a pool recorded beyond what Spark granted it.
 * @param pools
 *   The number of live memory pools.
 * @param plans
 *   The number of native plans created and not yet released.
 * @param jvmArrowAllocated
 *   The Arrow memory Comet holds on the JVM side.
 * @param jvmArrowImported
 *   The part of `jvmArrowAllocated` imported from native code, which `nativeAllocated` already
 *   counts.
 */
case class CometExecutorMemoryUsage(
    executorId: String,
    time: Long,
    nativeAllocated: Long,
    poolsReserved: Long,
    pools: Long,
    plans: Long,
    jvmArrowAllocated: Long,
    jvmArrowImported: Long)
    extends SparkListenerEvent

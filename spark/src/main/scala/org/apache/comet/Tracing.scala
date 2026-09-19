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

object Tracing {

  private val nativeLib = new Native

  /**
   * Emits the Arrow memory counters for the JVM side. `jvm_arrow_imported` is what the FFI import
   * path holds, which is mostly buffers imported from native over the C Data Interface, so the
   * difference is a close lower bound on the Arrow memory the JVM allocated itself. See
   * [[CometArrowImportAllocator]] for the JVM-allocated bytes that also land there.
   */
  def logArrowMemory(): Unit = {
    nativeLib.logMemoryUsage("jvm_arrow_allocated", CometArrowAllocator.getAllocatedMemory)
    nativeLib.logMemoryUsage("jvm_arrow_imported", CometArrowImportAllocator.getAllocatedMemory)
  }

  def withTrace[T](label: String, tracingEnabled: Boolean, fun: => T): T = {
    try {
      if (tracingEnabled) {
        nativeLib.traceBegin(label)
      }
      fun
    } finally {
      if (tracingEnabled) {
        nativeLib.traceEnd(label)
      }
    }
  }

}

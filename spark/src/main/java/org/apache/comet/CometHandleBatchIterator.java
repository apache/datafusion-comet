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

package org.apache.comet;

/**
 * Iterator that passes opaque native batch handles between two native execution contexts through
 * the JVM. Used when a native child plan feeds directly into a native ShuffleWriter, avoiding Arrow
 * FFI export/import overhead.
 *
 * <p>Called from native ScanExec via JNI. The source CometExecIterator must be in stash mode.
 */
public class CometHandleBatchIterator {
  private final CometExecIterator source;

  public CometHandleBatchIterator(CometExecIterator source) {
    this.source = source;
  }

  /**
   * Get the next batch handle from the source iterator.
   *
   * @return a native batch handle (positive long), or -1 if no more batches.
   */
  public long nextHandle() {
    return source.nextHandle();
  }

  /**
   * Get the native execution context pointer for the source (producer) plan. The consuming native
   * plan uses this to access the producer's batch stash.
   *
   * @return the native plan handle (execution context pointer) of the producer.
   */
  public long getSourceNativePlan() {
    return source.getNativePlan();
  }
}

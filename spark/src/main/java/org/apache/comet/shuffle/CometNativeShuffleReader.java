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

package org.apache.comet.shuffle;

/**
 * A shuffle reader that can expose encoded shuffle blocks directly to Comet's native execution
 * engine.
 *
 * <p>This interface enables the native shuffle scan path to consume and decode shuffle blocks via
 * JNI, bypassing JVM-side conversion to columnar batches and Arrow FFI.
 */
public interface CometNativeShuffleReader {

  /**
   * Creates an iterator over the encoded shuffle blocks selected by this reader.
   *
   * <p>The caller is responsible for closing the returned iterator after consumption.
   *
   * @return an iterator that supplies shuffle blocks to the native execution engine
   */
  CometShuffleBlockIterator readAsShuffleBlockIterator();
}

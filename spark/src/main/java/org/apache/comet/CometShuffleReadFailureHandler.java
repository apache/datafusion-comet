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

import java.io.IOException;

/**
 * Lets an input stream attribute native codec/IPC failures to its remote shuffle generation.
 *
 * <p>The native and JVM decoders invoke this only when decoding a fetched block. The stream itself
 * classifies fetch and frame-boundary errors: metadata timeouts and cleanup failures must not be
 * reclassified as corruption by this callback. If the handler returns, the caller propagates the
 * original failure. Local shuffle streams need not implement this interface.
 */
public interface CometShuffleReadFailureHandler {
  void onShuffleReadFailure(Throwable cause) throws IOException;
}

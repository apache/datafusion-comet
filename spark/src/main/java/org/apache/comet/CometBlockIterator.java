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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/** JNI protocol for iterators that expose codec-prefixed Arrow IPC blocks to native code. */
public interface CometBlockIterator extends Closeable {

  int hasNext() throws IOException;

  ByteBuffer getBuffer();

  int getCurrentBlockLength();

  void onDecodeFailure(String message) throws IOException;

  boolean requiresValidation();

  void incRecordsRead(long records);
}

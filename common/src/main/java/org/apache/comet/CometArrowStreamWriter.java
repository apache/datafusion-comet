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
import java.nio.channels.WritableByteChannel;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/**
 * A custom `ArrowStreamWriter` that allows writing batches from different root to the same stream.
 * Arrow `ArrowStreamWriter` cannot change the root after initialization.
 */
public class CometArrowStreamWriter extends ArrowStreamWriter {
  public CometArrowStreamWriter(
      VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
    super(root, provider, out);
  }

  public void writeMoreBatch(VectorSchemaRoot root) throws IOException {
    VectorUnloader unloader =
        new VectorUnloader(
            root, /*includeNullCount*/ true, NoCompressionCodec.INSTANCE, /*alignBuffers*/ true);

    try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
      writeRecordBatch(batch);
    }
  }
}

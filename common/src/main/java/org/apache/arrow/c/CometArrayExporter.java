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

package org.apache.arrow.c;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.c.jni.JniWrapper;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

import static org.apache.arrow.c.Data.exportField;
import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.c.NativeUtil.addressOrNull;
import static org.apache.arrow.util.Preconditions.checkNotNull;

public final class CometArrayExporter {
  // Copied from Data.exportVector and changed to take nullCount from outside
  public static void exportVector(
      BufferAllocator allocator,
      FieldVector vector,
      DictionaryProvider provider,
      ArrowArray out,
      ArrowSchema outSchema,
      long nullCount) {
    exportField(allocator, vector.getField(), provider, outSchema);
    export(allocator, out, vector, provider, nullCount);
  }

  private static void export(
      BufferAllocator allocator,
      ArrowArray array,
      FieldVector vector,
      DictionaryProvider dictionaryProvider,
      long nullCount) {
    List<FieldVector> children = vector.getChildrenFromFields();
    List<ArrowBuf> buffers = vector.getFieldBuffers();
    int valueCount = vector.getValueCount();
    DictionaryEncoding dictionaryEncoding = vector.getField().getDictionary();

    ArrayExporter.ExportedArrayPrivateData data = new ArrayExporter.ExportedArrayPrivateData();
    try {
      if (children != null) {
        data.children = new ArrayList<>(children.size());
        data.children_ptrs = allocator.buffer((long) children.size() * Long.BYTES);
        for (int i = 0; i < children.size(); i++) {
          ArrowArray child = ArrowArray.allocateNew(allocator);
          data.children.add(child);
          data.children_ptrs.writeLong(child.memoryAddress());
        }
      }

      if (buffers != null) {
        data.buffers = new ArrayList<>(buffers.size());
        data.buffers_ptrs = allocator.buffer((long) buffers.size() * Long.BYTES);
        vector.exportCDataBuffers(data.buffers, data.buffers_ptrs, NULL);
      }

      if (dictionaryEncoding != null) {
        Dictionary dictionary = dictionaryProvider.lookup(dictionaryEncoding.getId());
        checkNotNull(dictionary, "Dictionary lookup failed on export of dictionary encoded array");

        data.dictionary = ArrowArray.allocateNew(allocator);
        FieldVector dictionaryVector = dictionary.getVector();
        // Since the dictionary index tracks the nullCount, the nullCount of the values can be 0
        export(allocator, data.dictionary, dictionaryVector, dictionaryProvider, 0);
      }

      ArrowArray.Snapshot snapshot = new ArrowArray.Snapshot();
      snapshot.length = valueCount;
      snapshot.null_count = nullCount;
      snapshot.offset = 0;
      snapshot.n_buffers = (data.buffers != null) ? data.buffers.size() : 0;
      snapshot.n_children = (data.children != null) ? data.children.size() : 0;
      snapshot.buffers = addressOrNull(data.buffers_ptrs);
      snapshot.children = addressOrNull(data.children_ptrs);
      snapshot.dictionary = addressOrNull(data.dictionary);
      snapshot.release = NULL;
      array.save(snapshot);

      // sets release and private data
      JniWrapper.get().exportArray(array.memoryAddress(), data);
    } catch (Exception e) {
      data.close();
      throw e;
    }

    // Export children
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        FieldVector childVector = children.get(i);
        ArrowArray child = data.children.get(i);
        // TODO: getNullCount is slow, avoid calling it if possible
        int cNullCount = childVector.getNullCount();
        export(allocator, child, childVector, dictionaryProvider, cNullCount);
      }
    }
  }
}

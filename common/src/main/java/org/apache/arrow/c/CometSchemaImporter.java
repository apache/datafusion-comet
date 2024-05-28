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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/** This is a simple wrapper around SchemaImporter to make it accessible from Java Arrow. */
public class CometSchemaImporter {
  private final BufferAllocator allocator;
  private final SchemaImporter importer;
  private final CDataDictionaryProvider provider = new CDataDictionaryProvider();

  public CometSchemaImporter(BufferAllocator allocator) {
    this.allocator = allocator;
    this.importer = new SchemaImporter(allocator);
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public CDataDictionaryProvider getProvider() {
    return provider;
  }

  public Field importField(ArrowSchema schema) {
    try {
      return importer.importField(schema, provider);
    } finally {
      schema.release();
      schema.close();
    }
  }

  /**
   * Imports data from ArrowArray/ArrowSchema into a FieldVector. This is basically the same as Java
   * Arrow `Data.importVector`. `Data.importVector` initiates `SchemaImporter` internally which is
   * used to fill dictionary ids for dictionary encoded vectors. Every call to `importVector` will
   * begin with dictionary ids starting from 0. So, separate calls to `importVector` will overwrite
   * dictionary ids. To avoid this, we need to use the same `SchemaImporter` instance for all calls
   * to `importVector`.
   */
  public FieldVector importVector(ArrowArray array, ArrowSchema schema) {
    Field field = importField(schema);
    FieldVector vector = field.createVector(allocator);
    Data.importIntoVector(allocator, array, vector, provider);

    return vector;
  }

  public void close() {
    provider.close();
  }
}

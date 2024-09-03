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

/**
 * This class is used to import Arrow schema and array from native execution/shuffle. We cannot use
 * Arrow's Java API to import schema and array directly because Arrow's Java API `Data.importField`
 * initiates a new `SchemaImporter` for each field. Each `SchemaImporter` maintains an internal
 * dictionary id counter. So the dictionary ids for multiple dictionary columns will conflict with
 * each other and cause data corruption.
 */
public class ArrowImporter {
  private final SchemaImporter importer;
  private final BufferAllocator allocator;

  public ArrowImporter(BufferAllocator allocator) {
    this.allocator = allocator;
    this.importer = new SchemaImporter(allocator);
  }

  Field importField(ArrowSchema schema, CDataDictionaryProvider provider) {
    try {
      return importer.importField(schema, provider);
    } finally {
      schema.release();
      schema.close();
    }
  }

  public FieldVector importVector(
      ArrowArray array, ArrowSchema schema, CDataDictionaryProvider provider) {
    Field field = importField(schema, provider);
    FieldVector vector = field.createVector(allocator);
    CometArrayImporter importer = new CometArrayImporter(allocator, vector, provider);
    importer.importArray(array);
    return vector;
  }
}

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

import java.util.function.Function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
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
    return importVector(array, schema, provider, field -> field.createVector(allocator));
  }

  public FieldVector importVector(
      ArrowArray array,
      ArrowSchema schema,
      CDataDictionaryProvider provider,
      Function<Field, FieldVector> vectorFactory) {
    Field field = null;
    FieldVector vector = null;
    try {
      field = importField(schema, provider);
      vector = vectorFactory.apply(field);
      ArrayImporter importer = new ArrayImporter(allocator, vector, provider);
      importer.importArray(array);
      return vector;
    } catch (RuntimeException | Error failure) {
      if (vector != null) {
        AutoCloseables.close(failure, vector);
      }
      if (field != null) {
        closeDictionaries(field, provider, failure);
      }
      // ArrayImporter.importArray moves the array into a copy it owns and closes the source
      // before doImport can fail, so a closed array means the importer already took it and
      // releasing it again would be a double release.
      if (!array.isClosed()) {
        AutoCloseables.close(failure, array::release, array);
      }
      throw failure;
    }
  }

  /**
   * Closes the dictionary vectors registered in {@code provider} for {@code field} and its
   * children.
   *
   * <p>{@code ArrayImporter.doImport} loads a column's dictionary values before its main data, and
   * both sets of buffers hold references on the same imported C array. Those values live in the
   * provider rather than in the column's own vector, so closing the column alone leaves the C array
   * alive until the provider is closed at the end of the task. Each import assigns a fresh
   * dictionary id, so no other column can be using the entries this one created.
   */
  public static void closeDictionaries(
      Field field, CDataDictionaryProvider provider, Throwable failure) {
    DictionaryEncoding encoding = field.getDictionary();
    if (encoding != null) {
      Dictionary dictionary = provider.lookup(encoding.getId());
      if (dictionary != null) {
        AutoCloseables.close(failure, dictionary.getVector());
      }
    }
    for (Field child : field.getChildren()) {
      closeDictionaries(child, provider, failure);
    }
  }
}

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

package org.apache.comet.iceberg.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.arrow.c.AbstractCometSchemaImporter;

import org.apache.comet.CometSchemaImporter;
import org.apache.comet.IcebergApi;
import org.apache.comet.parquet.*;
import org.apache.comet.vector.CometVector;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that all @IcebergApi annotated elements are properly accessible. This test uses
 * reflection to scan for all annotated elements and ensures they meet the public API contract.
 */
public class IcebergApiVerificationTest extends AbstractApiTest {

  /** List of all classes that should have @IcebergApi annotations. */
  private static final List<Class<?>> ICEBERG_API_CLASSES =
      Arrays.asList(
          // Parquet classes
          FileReader.class,
          BatchReader.class,
          ColumnReader.class,
          ConstantColumnReader.class,
          MetadataColumnReader.class,
          AbstractColumnReader.class,
          RowGroupReader.class,
          ParquetColumnSpec.class,
          ReadOptions.class,
          ReadOptions.Builder.class,
          Utils.class,
          TypeUtil.class,
          WrappedInputFile.class,
          Native.class,
          // Vector classes
          CometVector.class,
          // Schema classes
          CometSchemaImporter.class,
          AbstractCometSchemaImporter.class);

  @Test
  public void testIcebergApiAnnotationIsRetainedAtRuntime() {
    Retention retention = IcebergApi.class.getAnnotation(Retention.class);
    assertThat(retention).isNotNull();
    assertThat(retention.value()).isEqualTo(RetentionPolicy.RUNTIME);
  }

  @Test
  public void testAllExpectedClassesHaveIcebergApiAnnotation() {
    List<String> missingAnnotations = new ArrayList<>();

    for (Class<?> clazz : ICEBERG_API_CLASSES) {
      if (!hasIcebergApiAnnotation(clazz)) {
        // Check if any methods, constructors, or fields have the annotation
        boolean hasAnyAnnotation = false;

        for (Method method : clazz.getDeclaredMethods()) {
          if (hasIcebergApiAnnotation(method)) {
            hasAnyAnnotation = true;
            break;
          }
        }

        if (!hasAnyAnnotation) {
          for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (hasIcebergApiAnnotation(constructor)) {
              hasAnyAnnotation = true;
              break;
            }
          }
        }

        if (!hasAnyAnnotation) {
          for (Field field : clazz.getDeclaredFields()) {
            if (hasIcebergApiAnnotation(field)) {
              hasAnyAnnotation = true;
              break;
            }
          }
        }

        if (!hasAnyAnnotation) {
          missingAnnotations.add(clazz.getName());
        }
      }
    }

    assertThat(missingAnnotations).as("Classes without @IcebergApi annotation").isEmpty();
  }

  @Test
  public void testAnnotatedClassesArePublic() {
    List<String> nonPublicClasses = new ArrayList<>();

    for (Class<?> clazz : ICEBERG_API_CLASSES) {
      if (hasIcebergApiAnnotation(clazz) && !isPublic(clazz)) {
        nonPublicClasses.add(clazz.getName());
      }
    }

    assertThat(nonPublicClasses).as("@IcebergApi annotated classes that are not public").isEmpty();
  }

  @Test
  public void testAnnotatedMethodsArePublic() {
    List<String> nonPublicMethods = new ArrayList<>();

    for (Class<?> clazz : ICEBERG_API_CLASSES) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (hasIcebergApiAnnotation(method) && !isPublic(method)) {
          nonPublicMethods.add(clazz.getSimpleName() + "." + method.getName());
        }
      }
    }

    assertThat(nonPublicMethods).as("@IcebergApi annotated methods that are not public").isEmpty();
  }

  @Test
  public void testAnnotatedConstructorsArePublic() {
    List<String> nonPublicConstructors = new ArrayList<>();

    for (Class<?> clazz : ICEBERG_API_CLASSES) {
      for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
        if (hasIcebergApiAnnotation(constructor) && !isPublic(constructor)) {
          nonPublicConstructors.add(
              clazz.getSimpleName() + "(" + Arrays.toString(constructor.getParameterTypes()) + ")");
        }
      }
    }

    assertThat(nonPublicConstructors)
        .as("@IcebergApi annotated constructors that are not public")
        .isEmpty();
  }

  @Test
  public void testAnnotatedFieldsAreAccessible() {
    List<String> inaccessibleFields = new ArrayList<>();

    for (Class<?> clazz : ICEBERG_API_CLASSES) {
      for (Field field : clazz.getDeclaredFields()) {
        if (hasIcebergApiAnnotation(field) && !isAccessible(field)) {
          inaccessibleFields.add(clazz.getSimpleName() + "." + field.getName());
        }
      }
    }

    assertThat(inaccessibleFields)
        .as("@IcebergApi annotated fields that are not accessible")
        .isEmpty();
  }

  @Test
  public void testFileReaderHasExpectedPublicApi() {
    Class<?> clazz = FileReader.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    // Check for expected public methods
    assertMethodExists(clazz, "readNextRowGroup");
    assertMethodExists(clazz, "skipNextRowGroup");
    assertMethodExists(clazz, "setRequestedSchemaFromSpecs", List.class);
    assertMethodExists(clazz, "close");
  }

  @Test
  public void testBatchReaderHasExpectedPublicApi() {
    Class<?> clazz = BatchReader.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    // Check for expected public methods
    assertMethodExists(clazz, "setSparkSchema", org.apache.spark.sql.types.StructType.class);
    assertMethodExists(clazz, "getColumnReaders");
    assertMethodExists(clazz, "nextBatch", int.class);

    // Check for expected constructor
    assertConstructorExists(clazz, AbstractColumnReader[].class);
  }

  @Test
  public void testColumnReaderHasExpectedPublicApi() {
    Class<?> clazz = ColumnReader.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    // Check for expected public methods
    assertMethodExists(clazz, "setPageReader", org.apache.parquet.column.page.PageReader.class);
    assertMethodExists(clazz, "setRowGroupReader", RowGroupReader.class, ParquetColumnSpec.class);
  }

  @Test
  public void testParquetColumnSpecHasExpectedPublicApi() {
    Class<?> clazz = ParquetColumnSpec.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    // Check for expected getter methods
    assertMethodExists(clazz, "getFieldId");
    assertMethodExists(clazz, "getPath");
    assertMethodExists(clazz, "getPhysicalType");
    assertMethodExists(clazz, "getTypeLength");
    assertMethodExists(clazz, "getMaxRepetitionLevel");
    assertMethodExists(clazz, "getMaxDefinitionLevel");
    assertMethodExists(clazz, "getLogicalTypeName");
    assertMethodExists(clazz, "getLogicalTypeParams");
  }

  @Test
  public void testReadOptionsHasExpectedPublicApi() {
    Class<?> clazz = ReadOptions.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    // Check for builder method
    assertMethodExists(clazz, "builder", org.apache.hadoop.conf.Configuration.class);

    // Check Builder class
    Class<?> builderClass = ReadOptions.Builder.class;
    assertThat(hasIcebergApiAnnotation(builderClass)).isTrue();
    assertMethodExists(builderClass, "build");
  }

  @Test
  public void testRowGroupReaderHasExpectedPublicApi() {
    Class<?> clazz = RowGroupReader.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    assertMethodExists(clazz, "getRowCount");
  }

  @Test
  public void testUtilsHasExpectedPublicApi() {
    Class<?> clazz = Utils.class;

    // Check for expected static methods
    assertStaticMethodExists(
        clazz,
        "getColumnReader",
        org.apache.spark.sql.types.DataType.class,
        ParquetColumnSpec.class,
        CometSchemaImporter.class,
        int.class,
        boolean.class,
        boolean.class,
        boolean.class);

    assertStaticMethodExists(clazz, "buildColumnDescriptor", ParquetColumnSpec.class);
    assertStaticMethodExists(
        clazz, "descriptorToParquetColumnSpec", org.apache.parquet.column.ColumnDescriptor.class);
  }

  @Test
  public void testTypeUtilHasExpectedPublicApi() {
    Class<?> clazz = TypeUtil.class;

    assertStaticMethodExists(
        clazz, "convertToParquet", org.apache.spark.sql.types.StructField.class);
  }

  @Test
  public void testCometVectorHasExpectedPublicApi() {
    Class<?> clazz = CometVector.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();
    assertThat(Modifier.isAbstract(clazz.getModifiers())).isTrue();

    // Check for expected methods
    assertMethodExists(clazz, "setNumNulls", int.class);
    assertMethodExists(clazz, "setNumValues", int.class);
    assertMethodExists(clazz, "numValues");
    assertMethodExists(clazz, "getValueVector");
    assertMethodExists(clazz, "slice", int.class, int.class);
  }

  @Test
  public void testCometSchemaImporterHasExpectedPublicApi() {
    Class<?> clazz = CometSchemaImporter.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    // Check for expected constructor
    assertConstructorExists(clazz, org.apache.arrow.memory.BufferAllocator.class);
  }

  @Test
  public void testWrappedInputFileHasExpectedPublicApi() {
    Class<?> clazz = WrappedInputFile.class;
    assertThat(hasIcebergApiAnnotation(clazz)).isTrue();
    assertThat(isPublic(clazz)).isTrue();

    // Check for expected constructor
    assertConstructorExists(clazz, Object.class);
  }

  @Test
  public void testNativeHasExpectedPublicApi() {
    Class<?> clazz = Native.class;

    // Check for expected native methods used by Iceberg
    assertStaticMethodExists(clazz, "resetBatch", long.class);
    assertStaticMethodExists(clazz, "setPosition", long.class, long.class, int.class);
    assertStaticMethodExists(clazz, "setIsDeleted", long.class, boolean[].class);
  }

  private void assertMethodExists(Class<?> clazz, String methodName, Class<?>... paramTypes) {
    try {
      Method method = clazz.getMethod(methodName, paramTypes);
      assertThat(method).isNotNull();
    } catch (NoSuchMethodException e) {
      throw new AssertionError(
          "Expected method " + methodName + " not found in " + clazz.getSimpleName(), e);
    }
  }

  private void assertStaticMethodExists(Class<?> clazz, String methodName, Class<?>... paramTypes) {
    try {
      Method method = clazz.getMethod(methodName, paramTypes);
      assertThat(method).isNotNull();
      assertThat(Modifier.isStatic(method.getModifiers()))
          .as(methodName + " should be static")
          .isTrue();
    } catch (NoSuchMethodException e) {
      throw new AssertionError(
          "Expected static method " + methodName + " not found in " + clazz.getSimpleName(), e);
    }
  }

  private void assertConstructorExists(Class<?> clazz, Class<?>... paramTypes) {
    try {
      Constructor<?> constructor = clazz.getConstructor(paramTypes);
      assertThat(constructor).isNotNull();
    } catch (NoSuchMethodException e) {
      throw new AssertionError(
          "Expected constructor with params "
              + Arrays.toString(paramTypes)
              + " not found in "
              + clazz.getSimpleName(),
          e);
    }
  }
}

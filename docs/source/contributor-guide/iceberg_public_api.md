<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Public API for Apache Iceberg Integration

This document describes the Comet classes and methods that form the public API used by
[Apache Iceberg](https://iceberg.apache.org/). These APIs enable Iceberg to leverage Comet's
native Parquet reader for vectorized reads in Spark.

**Important**: Changes to these APIs may break Iceberg's Comet integration. Contributors should
exercise caution when modifying these classes and consider backward compatibility.

All classes and methods documented here are marked with the `@IcebergApi` annotation
(`org.apache.comet.IcebergApi`) to make them easily identifiable in the source code.

## Overview

Iceberg uses Comet's Parquet reading infrastructure to accelerate table scans. The integration
uses two approaches:

1. **Hybrid Reader**: Native Parquet decoding with JVM-based I/O (requires building Iceberg
   from source with Comet patches)
2. **Native Reader**: Fully-native Iceberg scans using iceberg-rust

This document focuses on the Hybrid Reader API, which is used when Iceberg is configured with
`spark.sql.iceberg.parquet.reader-type=COMET`.

## Package: `org.apache.comet.parquet`

### FileReader

Main class for reading Parquet files with native decoding.

```java
// Constructor
public FileReader(
    WrappedInputFile inputFile,
    ReadOptions options,
    Map<String, String> properties,
    Long start,
    Long length,
    byte[] fileEncryptionKey,
    byte[] fileAADPrefix
) throws IOException

// Methods used by Iceberg
public void setRequestedSchemaFromSpecs(List<ParquetColumnSpec> specs)
public RowGroupReader readNextRowGroup() throws IOException
public void skipNextRowGroup()
public void close() throws IOException
```

### RowGroupReader

Provides access to row group data.

```java
// Methods used by Iceberg
public long getRowCount()
```

### ReadOptions

Configuration for Parquet read operations.

```java
// Builder pattern
public static Builder builder(Configuration conf)

public class Builder {
    public ReadOptions build()
}
```

### WrappedInputFile

Wrapper that adapts Iceberg's `InputFile` interface to Comet's file reading infrastructure.

```java
// Constructor
public WrappedInputFile(org.apache.iceberg.io.InputFile inputFile)
```

### ParquetColumnSpec

Specification describing a Parquet column's schema information.

```java
// Constructor
public ParquetColumnSpec(
    int fieldId,
    String[] path,
    String physicalType,
    int typeLength,
    boolean isRepeated,
    int maxDefinitionLevel,
    int maxRepetitionLevel,
    String logicalTypeName,
    Map<String, String> logicalTypeParams
)

// Getters used by Iceberg
public int getFieldId()
public String[] getPath()
public String getPhysicalType()
public int getTypeLength()
public int getMaxDefinitionLevel()
public int getMaxRepetitionLevel()
public String getLogicalTypeName()
public Map<String, String> getLogicalTypeParams()
```

### AbstractColumnReader

Base class for column readers.

```java
// Protected field accessed by Iceberg subclasses
protected long nativeHandle

// Methods used by Iceberg
public void setBatchSize(int batchSize)
public void close()
```

### ColumnReader

Column reader for regular Parquet columns (extends `AbstractColumnReader`).

```java
// Methods used by Iceberg
public void setPageReader(PageReader pageReader) throws IOException
```

### BatchReader

Coordinates reading batches across multiple column readers.

```java
// Constructor
public BatchReader(AbstractColumnReader[] columnReaders)

// Methods used by Iceberg
public void setSparkSchema(StructType schema)
public AbstractColumnReader[] getColumnReaders()
public void nextBatch(int batchSize)
```

### MetadataColumnReader

Reader for metadata columns (used for Iceberg's delete and position columns).

```java
// Constructor
public MetadataColumnReader(
    DataType sparkType,
    ColumnDescriptor descriptor,
    boolean useDecimal128,
    boolean isConstant
)

// Methods used by Iceberg
public void readBatch(int total)
public CometVector currentBatch()

// Protected field accessed by subclasses
protected long nativeHandle
```

### ConstantColumnReader

Reader for columns with constant/default values (extends `MetadataColumnReader`).

```java
// Constructor
public ConstantColumnReader(
    DataType sparkType,
    ColumnDescriptor descriptor,
    Object value,
    boolean useDecimal128
)
```

### Native

JNI interface for native operations.

```java
// Static methods used by Iceberg
public static void resetBatch(long nativeHandle)
public static void setIsDeleted(long nativeHandle, boolean[] isDeleted)
public static void setPosition(long nativeHandle, long position, int total)
```

### TypeUtil

Utilities for Parquet type conversions.

```java
// Methods used by Iceberg
public static ColumnDescriptor convertToParquet(StructField sparkField)
```

### Utils

General utility methods.

```java
// Methods used by Iceberg
public static AbstractColumnReader getColumnReader(
    DataType sparkType,
    ColumnDescriptor descriptor,
    CometSchemaImporter importer,
    int batchSize,
    boolean useDecimal128,
    boolean isConstant
)
```

## Package: `org.apache.comet`

### CometSchemaImporter

Imports and converts schemas between Arrow and Spark formats.

```java
// Constructor
public CometSchemaImporter(RootAllocator allocator)

// Methods used by Iceberg (inherited from AbstractCometSchemaImporter)
public void close()
```

## Package: `org.apache.arrow.c`

### AbstractCometSchemaImporter

Base class for `CometSchemaImporter`.

```java
// Methods used by Iceberg
public void close()
```

## Package: `org.apache.comet.vector`

### CometVector

Base class for Comet's columnar vectors (extends Spark's `ColumnVector`).

```java
// Constructor
public CometVector(DataType type, boolean useDecimal128)

// Abstract methods that subclasses must implement
public abstract int numValues()
public abstract ValueVector getValueVector()
public abstract CometVector slice(int offset, int length)
public abstract void setNumNulls(int numNulls)
public abstract void setNumValues(int numValues)

// Inherited from Spark ColumnVector - commonly overridden
public abstract void close()
public abstract boolean hasNull()
public abstract int numNulls()
public abstract boolean isNullAt(int rowId)
public abstract boolean getBoolean(int rowId)
// ... other type-specific getters
```

## Package: `org.apache.comet.shaded.arrow.memory`

### RootAllocator

Arrow memory allocator (shaded to avoid conflicts).

```java
// Constructor used by Iceberg
public RootAllocator()
```

## Package: `org.apache.comet.shaded.arrow.vector`

### ValueVector

Arrow's base vector interface (shaded). Used as return type in `CometVector.getValueVector()`.

## How Iceberg Uses These APIs

### Parquet File Reading Flow

1. Iceberg creates a `WrappedInputFile` from its `InputFile`
2. Creates `ReadOptions` via builder pattern
3. Instantiates `FileReader` with the wrapped input file
4. Converts Parquet `ColumnDescriptor`s to `ParquetColumnSpec`s using `CometTypeUtils`
5. Calls `setRequestedSchemaFromSpecs()` to specify which columns to read
6. Iterates through row groups via `readNextRowGroup()` and `skipNextRowGroup()`

### Column Reading Flow

1. Creates `CometSchemaImporter` with a `RootAllocator`
2. Uses `Utils.getColumnReader()` to create appropriate column readers
3. Calls `reset()` and `setPageReader()` for each row group
4. Uses `BatchReader` to coordinate reading batches across all columns
5. Retrieves results via `delegate().currentBatch()`

### Metadata Columns

Iceberg uses `MetadataColumnReader` subclasses for special columns:

- **Delete tracking**: Uses `Native.setIsDeleted()` to mark deleted rows
- **Position tracking**: Uses `Native.setPosition()` to track row positions

## Compatibility Considerations

When modifying these APIs, consider:

1. **Constructor signatures**: Adding required parameters breaks Iceberg
2. **Method signatures**: Changing return types or parameters breaks Iceberg
3. **Protected fields**: `MetadataColumnReader.nativeHandle` is accessed by Iceberg subclasses
4. **Shaded dependencies**: Arrow classes are shaded under `org.apache.comet.shaded`

## Testing Iceberg Integration

See the [Iceberg user guide](../user-guide/latest/iceberg.md) for instructions on testing
Comet with Iceberg.

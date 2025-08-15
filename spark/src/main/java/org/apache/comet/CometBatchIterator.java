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

import scala.collection.Iterator;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import org.apache.comet.vector.NativeUtil;

/**
 * A Java adapter iterator that provides batch-by-batch Arrow array access for native code consumption.
 * This class serves as a critical bridge between Spark's ColumnarBatch format and native DataFusion
 * execution, managing Arrow array ownership transfer across the JNI boundary.
 *
 * <h2>Architecture Role</h2>
 * CometBatchIterator acts as a pull-based data source for native execution:
 * <ul>
 * <li>Wraps Spark ColumnarBatch iterators from upstream operators</li>
 * <li>Exports Arrow arrays to native code via memory addresses</li>
 * <li>Manages ownership transfer using Arrow's C Data Interface</li>
 * <li>Provides JNI-friendly API for native code consumption</li>
 * </ul>
 *
 * <h2>Memory Ownership Model</h2>
 * The iterator implements a sophisticated ownership transfer pattern:
 * <pre>
 * JVM Phase:     CometBatchIterator owns ColumnarBatch
 *                        ↓
 * Export Phase:  Arrays exported via memory addresses
 *                        ↓  
 * Native Phase:  Native code takes ownership of Arrow arrays
 *                        ↓
 * Release Phase: Native code releases via Arrow C callbacks
 * </pre>
 *
 * <h2>JNI Integration</h2>
 * This class is designed specifically for JNI consumption by native Rust code:
 * <ul>
 * <li>{@code hasNext()} returns row count as int (JNI-friendly)</li>
 * <li>{@code next()} takes long arrays for memory addresses</li>
 * <li>No complex object passing across JNI boundary</li>
 * <li>Efficient zero-copy data transfer via memory addresses</li>
 * </ul>
 *
 * <h2>Usage Pattern</h2>
 * This class is typically used internally by {@link CometExecIterator} and should not
 * be used directly by application code:
 * <pre>{@code
 * // Internal usage by CometExecIterator
 * CometBatchIterator batchIterator = new CometBatchIterator(inputIterator, nativeUtil);
 * 
 * // Native code calls via JNI:
 * int rowCount = batchIterator.hasNext();  // Check availability
 * if (rowCount > 0) {
 *     long[] arrayAddrs = new long[numCols];
 *     long[] schemaAddrs = new long[numCols]; 
 *     int rows = batchIterator.next(arrayAddrs, schemaAddrs);  // Export arrays
 *     // Native code now owns the arrays
 * }
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * This class is <strong>NOT thread-safe</strong>. It's designed for single-threaded access
 * from native code via JNI. Concurrent access will cause race conditions and memory corruption.
 *
 * <h2>Memory Safety</h2>
 * <ul>
 * <li><strong>Single Consumption</strong>: Each batch is consumed exactly once</li>
 * <li><strong>Ownership Transfer</strong>: Ownership moves to native after {@code next()} call</li>
 * <li><strong>No Retention</strong>: No references to batches are held after export</li>
 * <li><strong>Automatic Cleanup</strong>: Native code handles Arrow array lifecycle</li>
 * </ul>
 *
 * @see CometExecIterator The primary execution iterator that uses this class
 * @see org.apache.comet.vector.NativeUtil Arrow array import/export utilities
 * @see <a href="https://arrow.apache.org/docs/format/CDataInterface.html">Arrow C Data Interface</a>
 *
 * @since 0.1.0
 * @author Apache DataFusion Comet Team
 */
public class CometBatchIterator {
  /** The upstream Spark ColumnarBatch iterator providing input data */
  final Iterator<ColumnarBatch> input;
  
  /** Utility class for Arrow array import/export operations via C Data Interface */
  final NativeUtil nativeUtil;
  
  /** 
   * Current batch being processed. null if no batch is available or after consumption.
   * This field manages the ownership state of the current batch.
   */
  private ColumnarBatch currentBatch = null;

  /**
   * Constructs a new CometBatchIterator wrapping the given input iterator.
   *
   * @param input The upstream Spark ColumnarBatch iterator to wrap
   * @param nativeUtil Utility for Arrow array operations and memory management
   */
  CometBatchIterator(Iterator<ColumnarBatch> input, NativeUtil nativeUtil) {
    this.input = input;
    this.nativeUtil = nativeUtil;
  }

  /**
   * Checks if there are more batches available and returns the row count of the next batch.
   *
   * <h3>Return Value Semantics</h3>
   * <ul>
   * <li><strong>Positive integer</strong>: Number of rows in the next available batch</li>
   * <li><strong>-1</strong>: No more batches available (end of iteration)</li>
   * </ul>
   *
   * <h3>Lazy Loading</h3>
   * This method implements lazy loading - it only fetches the next batch when called
   * and no current batch is available. This prevents unnecessary memory allocation
   * and allows for backpressure from native code.
   *
   * <h3>Memory Management</h3>
   * <ul>
   * <li>Does not consume the batch - only prepares it for consumption</li>
   * <li>Batch remains owned by this iterator until {@link #next} is called</li>
   * <li>Safe to call multiple times - returns same result until batch is consumed</li>
   * </ul>
   *
   * <h3>JNI Usage</h3>
   * This method is designed for JNI consumption by native Rust code:
   * <pre>{@code
   * // Native Rust code (via JNI):
   * int rowCount = batchIterator.hasNext();
   * if (rowCount > 0) {
   *     // Proceed to consume the batch
   *     batchIterator.next(arrayAddrs, schemaAddrs);
   * } else {
   *     // End of iteration
   * }
   * }</pre>
   *
   * @return Number of rows in next batch (>= 0) or -1 if no more batches
   *
   * @apiNote This method may trigger upstream computation if the input iterator
   *          performs lazy evaluation. Consider this for performance-sensitive code.
   */
  public int hasNext() {
    if (currentBatch == null) {
      if (input.hasNext()) {
        currentBatch = input.next();
      }
    }
    if (currentBatch == null) {
      return -1;
    } else {
      return currentBatch.numRows();
    }
  }

  /**
   * Exports the current batch's Arrow arrays to native code via memory addresses.
   *
   * <h3>Memory Ownership Transfer</h3>
   * This method implements a critical ownership transfer:
   * <ol>
   * <li><strong>Before call</strong>: CometBatchIterator owns the ColumnarBatch</li>
   * <li><strong>During call</strong>: Arrow arrays exported via C Data Interface</li> 
   * <li><strong>After call</strong>: Native code owns the Arrow array memory</li>
   * <li><strong>Cleanup</strong>: Native code responsible for releasing via Arrow callbacks</li>
   * </ol>
   *
   * <h3>Array Address Population</h3>
   * The method populates the provided address arrays with memory pointers:
   * <ul>
   * <li>{@code arrayAddrs[i]} = Memory address of ArrowArray struct for column i</li>
   * <li>{@code schemaAddrs[i]} = Memory address of ArrowSchema struct for column i</li>
   * </ul>
   *
   * <h3>Single Consumption Guarantee</h3>
   * Each batch can only be consumed once:
   * <ul>
   * <li>First call: Exports current batch and returns row count</li>
   * <li>Subsequent calls: Returns -1 (no batch available)</li>
   * <li>Must call {@link #hasNext} to prepare next batch</li>
   * </ul>
   *
   * <h3>Memory Safety</h3>
   * <ul>
   * <li><strong>Zero-copy transfer</strong>: No data copying, only pointer exchange</li>
   * <li><strong>Automatic cleanup</strong>: Arrow C callbacks handle memory release</li>
   * <li><strong>No use-after-free</strong>: Current batch nullified after export</li>
   * <li><strong>Exception safety</strong>: Failed exports don't corrupt state</li>
   * </ul>
   *
   * <h3>JNI Integration</h3>
   * This method is specifically designed for native code consumption via JNI:
   * <pre>{@code
   * // Native Rust code signature:
   * // fn consume_batch(array_addrs: &mut [i64], schema_addrs: &mut [i64]) -> i32
   * 
   * long[] arrayAddrs = new long[numColumns];
   * long[] schemaAddrs = new long[numColumns];
   * int rowCount = batchIterator.next(arrayAddrs, schemaAddrs);
   * 
   * // Native code now has access to Arrow arrays via addresses
   * // Arrays automatically released when native processing completes
   * }</pre>
   *
   * @param arrayAddrs Output array to receive ArrowArray struct memory addresses.
   *                   Must have length >= number of columns in the batch.
   * @param schemaAddrs Output array to receive ArrowSchema struct memory addresses.
   *                    Must have length >= number of columns in the batch.
   * @return Number of rows in the exported batch, or -1 if no batch is available
   *
   * @throws IllegalArgumentException if address arrays are too small for the batch schema
   * @throws IllegalStateException if Arrow export fails due to memory constraints
   *
   * @apiNote After this call, the caller (native code) owns the Arrow array memory
   *          and is responsible for proper cleanup via Arrow's release callbacks.
   */
  public int next(long[] arrayAddrs, long[] schemaAddrs) {
    if (currentBatch == null) {
      return -1;
    }
    int numRows = nativeUtil.exportBatch(arrayAddrs, schemaAddrs, currentBatch);
    currentBatch = null;
    return numRows;
  }
}

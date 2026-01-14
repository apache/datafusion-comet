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

package org.apache.spark.shuffle.sort;

import java.io.IOException;

import org.apache.spark.sql.comet.execution.shuffle.SpillInfo;

/**
 * Interface for Comet shuffle external sorters. This interface defines the public API that both
 * synchronous and asynchronous sorter implementations must provide.
 */
public interface CometShuffleExternalSorter {
  /** Writes a record to the shuffle sorter. */
  void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
      throws IOException;

  /** Sort and spill the current records in response to memory pressure. */
  void spill() throws IOException;

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter.
   */
  SpillInfo[] closeAndGetSpills() throws IOException;

  /** Returns the partition checksums. */
  long[] getChecksums();

  /** Returns the peak memory used so far, in bytes. */
  long getPeakMemoryUsedBytes();

  /** Force all memory and spill files to be deleted; called by shuffle error-handling code. */
  void cleanupResources();
}

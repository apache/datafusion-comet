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

package org.apache.arrow.memory;

import org.apache.arrow.memory.unsafe.UnsafeAllocationManager;
import org.apache.spark.unsafe.Platform;

/**
 * Builds allocators whose memory starts out holding a given byte in every position, rather than
 * whatever the system allocator hands back, so a test can tell what a writer left unwritten. It
 * lives in Arrow's package because the configuration that takes an allocation manager is not
 * public.
 */
public final class FilledMemoryAllocators {
  private FilledMemoryAllocators() {}

  public static RootAllocator create(byte fill) {
    AllocationManager.Factory factory =
        new AllocationManager.Factory() {
          @Override
          public AllocationManager create(BufferAllocator accountingAllocator, long size) {
            return new FilledAllocationManager(accountingAllocator, size, fill);
          }

          @Override
          public ArrowBuf empty() {
            return UnsafeAllocationManager.FACTORY.empty();
          }
        };
    return new RootAllocator(
        BaseAllocator.configBuilder().allocationManagerFactory(factory).build());
  }

  private static final class FilledAllocationManager extends AllocationManager {
    private final long size;
    private final long address;

    FilledAllocationManager(BufferAllocator accountingAllocator, long size, byte fill) {
      super(accountingAllocator);
      this.size = size;
      this.address = Platform.allocateMemory(size);
      Platform.setMemory(address, fill, size);
    }

    @Override
    public long getSize() {
      return size;
    }

    @Override
    protected long memoryAddress() {
      return address;
    }

    @Override
    protected void release0() {
      Platform.freeMemory(address);
    }
  }
}

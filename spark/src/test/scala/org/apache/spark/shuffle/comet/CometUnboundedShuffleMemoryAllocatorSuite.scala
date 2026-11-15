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

package org.apache.spark.shuffle.comet

import scala.collection.mutable.ArrayBuffer

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.unsafe.memory.MemoryBlock

class CometUnboundedShuffleMemoryAllocatorSuite extends AnyFunSuite {
  private val pageSize = 4096L

  private def newAllocator(): CometUnboundedShuffleMemoryAllocator = {
    val conf = new SparkConf(false).set("spark.memory.offHeap.enabled", "false")
    val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
    new CometUnboundedShuffleMemoryAllocator(taskMemoryManager, pageSize)
  }

  test("getUsed reports actual page sizes and ignores repeated frees") {
    val allocator = newAllocator()
    val pages = ArrayBuffer.empty[MemoryBlock]
    assert(allocator.getUsed === 0L)
    try {
      val small = allocator.allocate(1)
      pages += small
      assert(small.size() === pageSize)
      assert(allocator.getUsed === small.size())

      val oversized = allocator.allocate(pageSize + 1)
      pages += oversized
      assert(oversized.size() === pageSize + 1)
      assert(allocator.getUsed === small.size() + oversized.size())

      assert(allocator.free(small) === small.size())
      assert(allocator.getUsed === oversized.size())
      assert(allocator.free(small) === 0L)
      assert(allocator.getUsed === oversized.size())

      allocator.free(oversized)
      assert(allocator.getUsed === 0L)
    } finally {
      pages.foreach(allocator.free)
    }
  }

  test("getUsed includes pointer arrays and pages in the same total") {
    val allocator = newAllocator()
    val array = allocator.allocateArray(3)
    try {
      assert(array.memoryBlock().size() === 3L * java.lang.Long.BYTES)
      assert(allocator.getUsed === array.memoryBlock().size())
      val page = allocator.allocate(1)
      try {
        assert(allocator.getUsed === array.memoryBlock().size() + page.size())
        allocator.freeArray(array)
        assert(allocator.getUsed === page.size())
        allocator.freeArray(array)
        allocator.freeArray(null)
        assert(allocator.getUsed === page.size())
      } finally {
        allocator.free(page)
      }
      assert(allocator.getUsed === 0L)
    } finally {
      allocator.freeArray(array)
    }
  }

  test("allocations are not bounded by any budget") {
    // On-heap mode performs no memory accounting, so a request far larger than anything Comet
    // would have been granted under the old fixed-size pool succeeds.
    val allocator = newAllocator()
    val huge = 64L * 1024 * 1024
    val page = allocator.allocate(huge)
    try {
      assert(page.size() === huge)
      assert(allocator.getUsed === huge)
    } finally {
      allocator.free(page)
    }
    assert(allocator.getUsed === 0L)
  }

  test("an exhausted page table is reported as a refused acquisition") {
    // The page table is the only limit this allocator has. It has to surface as
    // SparkOutOfMemoryError so that the writers, which respond to that by spilling and retrying,
    // can free pages instead of failing the task.
    val allocator = newAllocator()
    val pages = ArrayBuffer.empty[MemoryBlock]
    val maxPages = 1 << 13
    try {
      for (_ <- 0 until maxPages) {
        pages += allocator.allocateArray(1).memoryBlock()
      }
      val allocatedBytes = pages.map(_.size()).sum
      assert(allocator.getUsed === allocatedBytes)
      intercept[SparkOutOfMemoryError] {
        allocator.allocateArray(1)
      }
      assert(allocator.getUsed === allocatedBytes)

      // Freeing a page makes room again, which is what lets a spilling writer make progress.
      allocator.free(pages.remove(pages.length - 1))
      val page = allocator.allocateArray(1).memoryBlock()
      pages += page
      assert(allocator.getUsed === allocatedBytes)
    } finally {
      pages.foreach(allocator.free)
    }
    assert(allocator.getUsed === 0L)
  }
}

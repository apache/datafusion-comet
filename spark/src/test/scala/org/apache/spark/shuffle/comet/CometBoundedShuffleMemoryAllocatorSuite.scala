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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.memory.MemoryBlock

import org.apache.comet.CometConf

class CometBoundedShuffleMemoryAllocatorSuite extends AnyFunSuite {
  private val pageSize = 4096L
  private val memoryLimit = 1024L * 1024

  private def newAllocator(): CometBoundedShuffleMemoryAllocator = {
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "false")
      .set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "1m")
    val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
    val sqlConf = new SQLConf
    sqlConf.setConfString(CometConf.COMET_SHUFFLE_JVM_MEMORY_FACTOR.key, "1.0")
    SQLConf.withExistingConf(sqlConf) {
      // Avoid the executor singleton so each test owns its budget and allocated pages.
      new CometBoundedShuffleMemoryAllocator(conf, taskMemoryManager, pageSize)
    }
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

  test("getUsed is unchanged when a partial grant is rolled back") {
    val allocator = newAllocator()
    val page = allocator.allocate(1)
    try {
      intercept[SparkOutOfMemoryError] {
        allocator.allocate(memoryLimit)
      }
      assert(allocator.getUsed === page.size())
    } finally {
      allocator.free(page)
    }
    assert(allocator.getUsed === 0L)
  }

  test("getUsed is unchanged when the budget is exhausted") {
    val allocator = newAllocator()
    val page = allocator.allocate(memoryLimit)
    try {
      assert(allocator.getUsed === memoryLimit)
      intercept[SparkOutOfMemoryError] {
        allocator.allocate(1)
      }
      assert(allocator.getUsed === memoryLimit)
    } finally {
      allocator.free(page)
    }
    assert(allocator.getUsed === 0L)
  }

  test("getUsed is unchanged when the page table is exhausted") {
    val allocator = newAllocator()
    val pages = ArrayBuffer.empty[MemoryBlock]
    val maxPages = 1 << 13
    try {
      for (_ <- 0 until maxPages) {
        pages += allocator.allocateArray(1).memoryBlock()
      }
      val allocatedBytes = pages.map(_.size()).sum
      assert(allocator.getUsed === allocatedBytes)
      intercept[IllegalStateException] {
        allocator.allocateArray(1)
      }
      assert(allocator.getUsed === allocatedBytes)
    } finally {
      pages.foreach(allocator.free)
    }
    assert(allocator.getUsed === 0L)
  }
}

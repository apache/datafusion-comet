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

package org.apache.spark.sql.comet.execution.arrow

import java.nio.file.{Files, Paths}
import java.util.Random

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.arrow.compression.ZstdCompressionCodec
import org.apache.arrow.memory.{AllocationListener, AllocationReservation, ArrowBuf, BufferAllocator, BufferManager, ForeignAllocation, RootAllocator}
import org.apache.arrow.memory.rounding.RoundingPolicy
import org.apache.arrow.vector.compression.CompressionUtil

/**
 * Checks that the zstd codec never writes past the buffer it allocates for its output.
 *
 * The buffers come from [[GuardedAllocator]], which hands the codec exactly the capacity it asked
 * for and keeps a sentinel-filled guard region right behind it, so an overrun shows up as a
 * changed sentinel instead of as silent corruption of whatever the allocator placed next (#7305).
 * The defect is explained on [[CometZstdCompressionCodec]]. The inputs are the ones that provoke
 * it: bytes that look structured enough for zstd to try entropy coding but do not compress.
 */
class CometZstdCompressionCodecSuite extends AnyFunSuite with Matchers {

  // zstd-jni unpacks its native library into java.io.tmpdir, which the test JVM points at
  // target/tmp. Nothing else creates that directory in a fresh checkout.
  Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))

  // The guard sees the overrun at any length, since the allocator cuts each buffer to the exact
  // size requested. This range just gives it many sizes to look at.
  private val Lengths = 400 to 600

  // Protobuf-like bytes: random payload with a scatter of repeated field tags. The tags make the
  // block pass zstd's "worth trying" heuristic, the payload makes the attempt not pay off, so
  // zstd runs its entropy coder to the end of the capacity it was given before storing the block.
  private def protoLike(len: Int, seed: Int): Array[Byte] = {
    val rnd = new Random(seed * 7919L + len)
    val out = new Array[Byte](len)
    rnd.nextBytes(out)
    (0 until 8 + rnd.nextInt(24)).foreach { _ =>
      out(rnd.nextInt(len)) = (0x08 + 8 * rnd.nextInt(4)).toByte
    }
    out
  }

  private def compressible(len: Int, seed: Int): Array[Byte] =
    Array.tabulate(len)(i => ((i / 7 + seed) % 5).toByte)

  private def roundTrip(
      codec: ZstdCompressionCodec,
      allocator: BufferAllocator,
      guarded: GuardedAllocator,
      bytes: Array[Byte]): Boolean = {
    val input = allocator.buffer(bytes.length.toLong)
    input.setBytes(0, bytes)
    input.writerIndex(bytes.length.toLong)
    // compress consumes the input reference and allocates its output from the guarded allocator.
    val compressed = codec.compress(guarded, input)
    val storedRaw = compressed.getLong(0) == CompressionUtil.NO_COMPRESSION_LENGTH
    val plain = codec.decompress(allocator, compressed)
    try {
      plain.writerIndex() shouldBe bytes.length
      val actual = new Array[Byte](bytes.length)
      plain.getBytes(0, actual)
      actual shouldBe bytes
    } finally {
      plain.close()
    }
    storedRaw
  }

  private def withAllocators(f: (BufferAllocator, GuardedAllocator) => Unit): Unit = {
    val root = new RootAllocator()
    val guarded = new GuardedAllocator(root)
    try f(root, guarded)
    finally {
      guarded.close()
      root.close()
    }
  }

  test("Arrow's zstd codec writes past the output buffer it allocates") {
    // Documents the defect this suite guards against (apache/arrow-java#1119, fixed in Arrow
    // 20.0.0). Once Comet moves to an Arrow with the fix, CometZstdCompressionCodec and this test
    // go together.
    withAllocators { (root, guarded) =>
      val codec = new ZstdCompressionCodec(1)
      Lengths.foreach { len =>
        (0 until 50).foreach(seed => roundTrip(codec, root, guarded, protoLike(len, seed)))
      }
      guarded.overruns should be > 0
    }
  }

  test("Comet's zstd codec stays within the output buffer it allocates") {
    withAllocators { (root, guarded) =>
      val codec = new CometZstdCompressionCodec(1)
      var raw = 0
      var packed = 0
      def count(storedRaw: Boolean): Unit = if (storedRaw) raw += 1 else packed += 1
      Lengths.foreach { len =>
        (0 until 50).foreach(seed => count(roundTrip(codec, root, guarded, protoLike(len, seed))))
        (0 until 5).foreach(seed =>
          count(roundTrip(codec, root, guarded, compressible(len, seed))))
      }
      guarded.overruns shouldBe 0
      // Both outcomes ran under the guard: the stored block the overrun comes from, and a block
      // that really compressed.
      raw should be > 0
      packed should be > 0
    }
  }
}

/**
 * Hands out buffers of exactly the requested capacity, each followed by a sentinel-filled guard
 * region, and counts the buffers whose guard was written to by the time they were released.
 *
 * Each buffer is a slice at the start of a larger allocation from `root`; the allocator keeps a
 * reference to the allocation so the guard can still be read once the codec has closed its slice.
 * Everything other than allocation delegates to `root`.
 */
private final class GuardedAllocator(root: BufferAllocator) extends BufferAllocator {
  private val Guard = 64
  private val Sentinel: Byte = 0x5a
  // Each allocation with the size that was requested from it, where its guard begins.
  private val parents = mutable.ArrayBuffer.empty[(ArrowBuf, Long)]

  /** Buffers whose guard region no longer holds the sentinel. */
  def overruns: Int = parents.count { case (parent, size) =>
    (0 until Guard).exists(i => parent.getByte(size + i) != Sentinel)
  }

  override def buffer(size: Long): ArrowBuf = {
    val parent = root.buffer(size + Guard)
    // The slice is cut to exactly `size`, whatever the allocator rounded the request to, and the
    // guard starts where the slice ends.
    parent.setBytes(size, Array.fill[Byte](Guard)(Sentinel))
    // The slice shares the parent's reference manager, so retain once for the reference this
    // allocator keeps; the codec's close releases the other.
    parent.getReferenceManager.retain()
    parents += ((parent, size))
    parent.slice(0, size).writerIndex(0)
  }

  override def buffer(size: Long, manager: BufferManager): ArrowBuf = buffer(size)

  override def close(): Unit = {
    parents.foreach(_._1.getReferenceManager.release())
    parents.clear()
  }

  override def getRoot: BufferAllocator = root.getRoot
  override def newChildAllocator(
      name: String,
      initReservation: Long,
      maxAllocation: Long): BufferAllocator =
    root.newChildAllocator(name, initReservation, maxAllocation)
  override def newChildAllocator(
      name: String,
      listener: AllocationListener,
      initReservation: Long,
      maxAllocation: Long): BufferAllocator =
    root.newChildAllocator(name, listener, initReservation, maxAllocation)
  override def getAllocatedMemory: Long = root.getAllocatedMemory
  override def getLimit: Long = root.getLimit
  override def getInitReservation: Long = root.getInitReservation
  override def setLimit(newLimit: Long): Unit = root.setLimit(newLimit)
  override def getPeakMemoryAllocation: Long = root.getPeakMemoryAllocation
  override def getHeadroom: Long = root.getHeadroom
  override def forceAllocate(size: Long): Boolean = root.forceAllocate(size)
  override def releaseBytes(size: Long): Unit = root.releaseBytes(size)
  override def getListener: AllocationListener = root.getListener
  override def getParentAllocator: BufferAllocator = root.getParentAllocator
  override def getChildAllocators: java.util.Collection[BufferAllocator] = root.getChildAllocators
  override def newReservation(): AllocationReservation = root.newReservation()
  override def getEmpty: ArrowBuf = root.getEmpty
  override def getName: String = "guarded"
  override def isOverLimit: Boolean = root.isOverLimit
  override def toVerboseString: String = root.toVerboseString
  override def assertOpen(): Unit = root.assertOpen()
  override def getRoundingPolicy: RoundingPolicy = root.getRoundingPolicy
  override def wrapForeignAllocation(allocation: ForeignAllocation): ArrowBuf =
    root.wrapForeignAllocation(allocation)
}

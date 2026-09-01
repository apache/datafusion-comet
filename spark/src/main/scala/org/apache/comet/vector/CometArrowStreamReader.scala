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

package org.apache.comet.vector

import java.nio.channels.ReadableByteChannel
import java.util

import scala.collection.JavaConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{FieldVector, VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.compression.CompressionCodec
import org.apache.arrow.vector.dictionary.Dictionary
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel}
import org.apache.arrow.vector.ipc.message.{ArrowDictionaryBatch, ArrowRecordBatch, MessageChannelReader}
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.arrow.vector.util.{DictionaryUtility, VectorBatchAppender}

/**
 * Arrow IPC reader that keeps struct children positional when a schema contains duplicate names.
 *
 * ArrowReader normally allocates each field with `Field.createVector`, which indexes direct
 * struct children by name and collapses duplicates. Reuse NativeUtil's import factory here so IPC
 * and C Data imports have the same physical layout and the ordinary no-duplicate path stays
 * unchanged.
 */
final class CometArrowStreamReader(
    messageReader: MessageChannelReader,
    allocator: BufferAllocator,
    compressionFactory: CompressionCodec.Factory)
    extends ArrowStreamReader(messageReader, allocator, compressionFactory) {

  def this(messageReader: MessageChannelReader, allocator: BufferAllocator) =
    this(messageReader, allocator, CompressionCodec.Factory.INSTANCE)

  def this(channel: ReadableByteChannel, allocator: BufferAllocator) =
    this(
      new MessageChannelReader(new ReadChannel(channel), allocator),
      allocator,
      CompressionCodec.Factory.INSTANCE)

  private var cometInitialized = false
  private var cometResourcesClosed = false
  private var cometSourceClosed = false
  private var cometRoot: VectorSchemaRoot = _
  private var cometLoader: VectorLoader = _

  override protected def initialize(): Unit = {
    val originalSchema = readSchema()
    val fields = new util.ArrayList[Field](originalSchema.getFields.size())
    val vectors = new util.ArrayList[FieldVector](originalSchema.getFields.size())
    val importedDictionaries = new util.HashMap[java.lang.Long, Dictionary]()

    try {
      originalSchema.getFields.asScala.foreach { field =>
        val updated = DictionaryUtility.toMemoryFormat(field, allocator, importedDictionaries)
        fields.add(updated)
        vectors.add(NativeUtil.createVectorForImport(updated, allocator))
      }
      cometRoot =
        new VectorSchemaRoot(new Schema(fields, originalSchema.getCustomMetadata), vectors, 0)
      cometLoader = new VectorLoader(cometRoot, compressionFactory)
      dictionaries = util.Collections.unmodifiableMap(importedDictionaries)
      cometInitialized = true
    } catch {
      case failure: Throwable =>
        AutoCloseables.close(failure, vectors)
        AutoCloseables.close(
          failure,
          importedDictionaries.values().asScala.map(_.getVector).asJava)
        cometRoot = null
        cometLoader = null
        throw failure
    }
  }

  override protected def ensureInitialized(): Unit = {
    if (!cometInitialized) initialize()
  }

  override def getVectorSchemaRoot: VectorSchemaRoot = {
    ensureInitialized()
    cometRoot
  }

  override def getDictionaryVectors: util.Map[java.lang.Long, Dictionary] = {
    ensureInitialized()
    dictionaries
  }

  override def lookup(id: Long): Dictionary = {
    if (!cometInitialized) {
      throw new IllegalStateException("Unable to lookup until reader has been initialized")
    }
    dictionaries.get(id)
  }

  override def getDictionaryIds: util.Set[java.lang.Long] = {
    if (!cometInitialized) {
      throw new IllegalStateException(
        "Unable to list dictionaries until reader has been initialized")
    }
    dictionaries.keySet()
  }

  override protected def prepareLoadNextBatch(): Unit = {
    ensureInitialized()
    cometRoot.setRowCount(0)
  }

  override protected def loadRecordBatch(batch: ArrowRecordBatch): Unit = {
    try cometLoader.load(batch)
    finally batch.close()
  }

  override protected def loadDictionary(dictionaryBatch: ArrowDictionaryBatch): Unit = {
    val dictionary = dictionaries.get(dictionaryBatch.getDictionaryId)
    if (dictionary == null) {
      throw new IllegalArgumentException(
        s"Dictionary ID ${dictionaryBatch.getDictionaryId} not defined in schema")
    }

    val vector = dictionary.getVector
    if (dictionaryBatch.isDelta) {
      val deltaVector = NativeUtil.createVectorForImport(vector.getField, allocator)
      try {
        loadDictionaryBatch(dictionaryBatch, deltaVector)
        VectorBatchAppender.batchAppend(vector, deltaVector)
      } finally {
        deltaVector.close()
      }
    } else {
      loadDictionaryBatch(dictionaryBatch, vector)
    }
  }

  private def loadDictionaryBatch(
      dictionaryBatch: ArrowDictionaryBatch,
      vector: FieldVector): Unit = {
    val root = new VectorSchemaRoot(
      util.Collections.singletonList(vector.getField),
      util.Collections.singletonList(vector),
      0)
    val loader = new VectorLoader(root, compressionFactory)
    try loader.load(dictionaryBatch.getDictionary)
    finally dictionaryBatch.close()
  }

  override def close(): Unit = close(closeReadSource = true)

  override def close(closeReadSource: Boolean): Unit = {
    val resources = new util.ArrayList[AutoCloseable]()
    if (!cometResourcesClosed) {
      cometResourcesClosed = true
      if (cometRoot != null) resources.add(cometRoot)
      if (dictionaries != null) {
        dictionaries.values().asScala.foreach(dictionary => resources.add(dictionary.getVector))
      }
    }
    if (closeReadSource && !cometSourceClosed) {
      cometSourceClosed = true
      resources.add(new AutoCloseable {
        override def close(): Unit = CometArrowStreamReader.super.closeReadSource()
      })
    }
    AutoCloseables.close(resources)
  }
}

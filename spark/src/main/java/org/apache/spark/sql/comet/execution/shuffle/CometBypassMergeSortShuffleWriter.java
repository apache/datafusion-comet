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

package org.apache.spark.sql.comet.execution.shuffle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import scala.*;
import scala.collection.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.comet.CometShuffleChecksumSupport;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.sort.CometShuffleExternalSorter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.util.Utils;

import com.google.common.io.Closeables;

import org.apache.comet.CometConf$;
import org.apache.comet.Native;

/**
 * This is based on Spark `BypassMergeSortShuffleWriter`. Instead of `DiskBlockObjectWriter`, this
 * writes Spark Internal Rows through `DiskBlockArrowIPCWriter` as Arrow IPC bytes. Note that Spark
 * `DiskBlockObjectWriter` is general writer for any objects, but `DiskBlockArrowIPCWriter` is
 * specialized for Spark Internal Rows and SQL workloads.
 */
final class CometBypassMergeSortShuffleWriter<K, V> extends ShuffleWriter<K, V>
    implements CometShuffleChecksumSupport {

  private static final Logger logger =
      LoggerFactory.getLogger(CometBypassMergeSortShuffleWriter.class);
  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final TaskMemoryManager memoryManager;
  private CometShuffleMemoryAllocatorTrait allocator;
  private final TaskContext taskContext;
  private final SerializerInstance serializer;

  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final int shuffleId;
  private final long mapId;
  private final ShuffleExecutorComponents shuffleExecutorComponents;
  private final StructType schema;

  /** Array of file writers, one for each partition */
  private CometDiskBlockWriter[] partitionWriters;

  private FileSegment[] partitionWriterSegments;
  private MapStatus mapStatus;
  private long[] partitionLengths;

  /** Checksum calculator for each partition. Empty when shuffle checksum disabled. */
  private final long[] partitionChecksums;

  private final boolean isAsync;

  private final int asyncThreadNum;

  /** Thread pool shared across all partition writers, for async write batch */
  private final ExecutorService threadPool;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc twice.
   */
  private boolean stopping = false;

  private final SparkConf conf;

  private boolean tracingEnabled;

  CometBypassMergeSortShuffleWriter(
      BlockManager blockManager,
      TaskMemoryManager memoryManager,
      TaskContext taskContext,
      CometBypassMergeSortShuffleHandle<K, V> handle,
      long mapId,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) {
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.conf = conf;
    this.blockManager = blockManager;
    this.memoryManager = memoryManager;
    this.taskContext = taskContext;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.mapId = mapId;
    this.serializer = dep.serializer().newInstance();
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = writeMetrics;
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.schema = ((CometShuffleDependency<?, ?, ?>) dep).schema().get();
    this.partitionChecksums = createPartitionChecksums(numPartitions, conf);

    this.isAsync = (boolean) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED().get();
    this.asyncThreadNum = (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_THREAD_NUM().get();
    this.tracingEnabled = (boolean) CometConf$.MODULE$.COMET_TRACING_ENABLED().get();

    if (isAsync) {
      logger.info("Async shuffle writer enabled");
      this.threadPool = ShuffleThreadPool.getThreadPool();
    } else {
      logger.info("Async shuffle writer disabled");
      this.threadPool = null;
    }
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    ShuffleMapOutputWriter mapOutputWriter =
        shuffleExecutorComponents.createMapOutputWriter(shuffleId, mapId, numPartitions);
    try {
      if (!records.hasNext()) {
        partitionLengths =
            mapOutputWriter
                .commitAllPartitions(ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE)
                .getPartitionLengths();
        mapStatus =
            MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths, mapId);
        return;
      }
      final long openStartTime = System.nanoTime();
      partitionWriters = new CometDiskBlockWriter[numPartitions];
      partitionWriterSegments = new FileSegment[numPartitions];

      final String checksumAlgorithm = getChecksumAlgorithm(conf);

      allocator =
          CometShuffleMemoryAllocator.getInstance(
              conf,
              memoryManager,
              Math.min(
                  CometShuffleExternalSorter.MAXIMUM_PAGE_SIZE_BYTES,
                  memoryManager.pageSizeBytes()));

      // Allocate the disk writers, and open the files that we'll be writing to
      for (int i = 0; i < numPartitions; i++) {
        final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
            blockManager.diskBlockManager().createTempShuffleBlock();
        final File file = tempShuffleBlockIdPlusFile._2();
        CometDiskBlockWriter writer =
            new CometDiskBlockWriter(
                file,
                allocator,
                taskContext,
                serializer,
                schema,
                writeMetrics,
                conf,
                isAsync,
                asyncThreadNum,
                threadPool,
                tracingEnabled);
        if (partitionChecksums.length > 0) {
          writer.setChecksum(partitionChecksums[i]);
          writer.setChecksumAlgo(checksumAlgorithm);
        }
        partitionWriters[i] = writer;
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

      long outputRows = 0;

      while (records.hasNext()) {
        outputRows += 1;

        final Product2<K, V> record = records.next();
        final K key = record._1();
        // Safety: `CometBypassMergeSortShuffleWriter` is only used when dealing with Comet shuffle
        // dependencies, which always produce `ColumnarBatch`es.
        int partition_id = partitioner.getPartition(key);
        partitionWriters[partitioner.getPartition(key)].insertRow(
            (UnsafeRow) record._2(), partition_id);
      }

      Native _native = new Native();
      if (tracingEnabled) {
        _native.logCounter("comet_shuffle_", allocator.getUsed());
      }

      long spillRecords = 0;

      for (int i = 0; i < numPartitions; i++) {
        CometDiskBlockWriter writer = partitionWriters[i];
        partitionWriterSegments[i] = writer.close();

        spillRecords += writer.getOutputRecords();
      }

      if (tracingEnabled) {
        _native.logCounter("comet_shuffle_", allocator.getUsed());
      }

      if (outputRows != spillRecords) {
        throw new RuntimeException(
            "outputRows("
                + outputRows
                + ") != spillRecords("
                + spillRecords
                + "). Please file a bug report.");
      }

      // TODO: We probably can move checksum generation here when concatenating partition files
      partitionLengths = writePartitionedData(mapOutputWriter);
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths, mapId);
    } catch (Exception e) {
      try {
        mapOutputWriter.abort(e);
      } catch (Exception e2) {
        logger.error("Failed to abort the writer after failing to write map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
  }

  @Override
  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedData(ShuffleMapOutputWriter mapOutputWriter) throws IOException {
    // Track location of the partition starts in the output file
    if (partitionWriters != null) {
      final long writeStartTime = System.nanoTime();
      final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();

      // Gets computed checksums for each partition
      for (int i = 0; i < partitionChecksums.length; i++) {
        partitionChecksums[i] = partitionWriters[i].getChecksum();
      }

      try {
        for (int i = 0; i < numPartitions; i++) {
          final File file = partitionWriterSegments[i].file();
          ShufflePartitionWriter writer = mapOutputWriter.getPartitionWriter(i);

          if (file.exists()) {
            if (transferToEnabled && !encryptionEnabled) {
              // Using WritableByteChannelWrapper to make resource closing consistent between
              // this implementation and UnsafeShuffleWriter.
              Optional<WritableByteChannelWrapper> maybeOutputChannel = writer.openChannelWrapper();
              if (maybeOutputChannel.isPresent()) {
                writePartitionedDataWithChannel(file, maybeOutputChannel.get());
              } else {
                writePartitionedDataWithStream(file, writer);
              }
            } else {
              writePartitionedDataWithStream(file, writer);
            }
            if (!file.delete()) {
              logger.error("Unable to delete file for partition {}", i);
            }
          }
        }
      } finally {
        writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
      }
      partitionWriters = null;
    }

    return mapOutputWriter.commitAllPartitions(partitionChecksums).getPartitionLengths();
  }

  private void writePartitionedDataWithChannel(File file, WritableByteChannelWrapper outputChannel)
      throws IOException {
    boolean copyThrewException = true;
    try {
      FileInputStream in = new FileInputStream(file);
      try (FileChannel inputChannel = in.getChannel()) {
        Utils.copyFileStreamNIO(inputChannel, outputChannel.channel(), 0L, inputChannel.size());
        copyThrewException = false;
      } finally {
        Closeables.close(in, copyThrewException);
      }
    } finally {
      Closeables.close(outputChannel, copyThrewException);
    }
  }

  private void writePartitionedDataWithStream(File file, ShufflePartitionWriter writer)
      throws IOException {
    boolean copyThrewException = true;
    FileInputStream in = new FileInputStream(file);
    OutputStream outputStream;
    try {
      outputStream = blockManager.serializerManager().wrapForEncryption(writer.openStream());

      try {
        Utils.copyStream(in, outputStream, false, false);
        copyThrewException = false;
      } finally {
        Closeables.close(outputStream, copyThrewException);
      }
    } finally {
      Closeables.close(in, copyThrewException);
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;

      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (CometDiskBlockWriter writer : partitionWriters) {
              writer.freeMemory();

              File file = writer.getFile();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}

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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import javax.annotation.Nullable;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.BaseShuffleHandle;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.sort.CometShuffleExternalSorter;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.apache.spark.shuffle.sort.UnsafeShuffleWriter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import org.apache.comet.CometConf;
import org.apache.comet.Native;

/**
 * This is based on Spark {@link UnsafeShuffleWriter}, as a writer to write shuffling rows into
 * Arrow format after sorting rows based on the partition ID.
 *
 * <p>While writing rows through this writer, it writes rows into {@link CometShuffleExternalSorter}
 * which buffers rows in memory (`ShuffleInMemorySorter`). When the memory buffer is full, it will
 * sort rows based on the partition ID and write them into a spill file. In Spark, sorting is
 * performed by `ShuffleInMemorySorter`. In Comet, if off-heap memory is enabled, and radix sort is
 * enabled, Comet will sort rows through native code. Sorting is based on a long array containing
 * compacted partition IDs and row pointers. The row pointers are the addresses of the rows in the
 * off-heap memory. After sorting, Comet will write the sorted rows into a spill file through native
 * code (off-heap memory must be enabled).
 *
 * <p>In native code, Comet converts UnsafeRows to Arrow arrays and writes arrays into the spill
 * file with Arrow IPC format.
 */
@Private
public class CometUnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  @VisibleForTesting static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;
  private static final Logger logger = LoggerFactory.getLogger(CometUnsafeShuffleWriter.class);
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();
  private final BlockManager blockManager;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final ShuffleExecutorComponents shuffleExecutorComponents;
  private final int shuffleId;
  private final long mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;
  private final StructType schema;

  @Nullable private MapStatus mapStatus;
  @Nullable private CometShuffleExternalSorter sorter;

  @Nullable private long[] partitionLengths;

  private long peakMemoryUsedBytes = 0;
  private ExposedByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;
  private Native nativeLib = new Native();
  private CometShuffleMemoryAllocatorTrait allocator;
  private boolean tracingEnabled;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true and
   * then call stop() with success = false if they get an exception, we want to make sure we don't
   * try deleting files, etc twice.
   */
  private boolean stopping = false;

  public CometUnsafeShuffleWriter(
      BlockManager blockManager,
      TaskMemoryManager memoryManager,
      BaseShuffleHandle<K, V, V> handle,
      long mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      ShuffleWriteMetricsReporter writeMetrics,
      ShuffleExecutorComponents shuffleExecutorComponents) {
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
          "CometUnsafeShuffleWriter can only be used for shuffles with at most "
              + SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()
              + " reduce partitions");
    }
    this.blockManager = blockManager;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.schema = (StructType) ((CometShuffleDependency) dep).schema().get();
    this.writeMetrics = writeMetrics;
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    this.initialSortBufferSize =
        (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
    this.inputBufferSizeInBytes =
        (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.tracingEnabled = (boolean) CometConf.COMET_TRACING_ENABLED().get();
    open();
  }

  private static OutputStream openStreamUnchecked(ShufflePartitionWriter writer) {
    try {
      return writer.openStream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /** Return the peak memory used so far, in bytes. */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /** This convenience method should only be called in test code. */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    boolean success = false;
    if (tracingEnabled) {
      nativeLib.traceBegin("CometUnsafeShuffleWriter");
    }
    String offheapMemKey = "comet_shuffle_" + Thread.currentThread().getId();
    try {
      while (records.hasNext()) {
        insertRecordIntoSorter(records.next());
      }
      if (tracingEnabled) {
        nativeLib.logMemoryUsage(offheapMemKey, this.allocator.getUsed());
      }
      closeAndWriteOutput();
      success = true;
    } finally {
      if (tracingEnabled) {
        nativeLib.traceEnd("CometUnsafeShuffleWriter");
      }
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error(
                "In addition to a failure during writing, we failed during " + "cleanup.", e);
          }
        }
      }
      if (tracingEnabled) {
        nativeLib.logMemoryUsage(offheapMemKey, this.allocator.getUsed());
      }
    }
  }

  private void open() {
    assert (allocator == null);
    assert (sorter == null);
    allocator =
        CometShuffleMemoryAllocator.getInstance(
            sparkConf,
            memoryManager,
            Math.min(
                CometShuffleExternalSorter.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()));
    sorter =
        new CometShuffleExternalSorter(
            allocator,
            blockManager,
            taskContext,
            initialSortBufferSize,
            partitioner.numPartitions(),
            sparkConf,
            writeMetrics,
            schema);
    serBuffer = new ExposedByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert (sorter != null);
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    try {
      partitionLengths = mergeSpills(spills);
    } finally {
      sorter = null;
      for (SpillInfo spill : spills) {
        if (spill.file.exists() && !spill.file.delete()) {
          logger.error("Error while deleting spill file {}", spill.file.getPath());
        }
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths, mapId);
  }

  @VisibleForTesting
  /** Serializes records and inserts into external sorter */
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert (sorter != null);
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);
    serBuffer.reset();
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue((UnsafeRow) record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    sorter.insertRecord(
        serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

  /**
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file.
   */
  private long[] mergeSpills(SpillInfo[] spills) throws IOException {
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();

    long[] partitionLengths;
    if (spills.length == 0) {
      final ShuffleMapOutputWriter mapWriter =
          shuffleExecutorComponents.createMapOutputWriter(
              shuffleId, mapId, partitioner.numPartitions());
      return mapWriter
          .commitAllPartitions(ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE)
          .getPartitionLengths();
    } else if (spills.length == 1) {
      Optional<SingleSpillShuffleMapOutputWriter> maybeSingleFileWriter =
          shuffleExecutorComponents.createSingleFileMapOutputWriter(shuffleId, mapId);
      if (maybeSingleFileWriter.isPresent() && !encryptionEnabled) {
        // Comet native writer doesn't perform encryption. If encryption is enabled, we should
        // perform spill merging which will perform encryption.

        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        partitionLengths = spills[0].partitionLengths;
        logger.debug(
            "Merge shuffle spills for mapId {} with length {}", mapId, partitionLengths.length);
        maybeSingleFileWriter
            .get()
            .transferMapSpillFile(spills[0].file, partitionLengths, sorter.getChecksums());
      } else {
        partitionLengths = mergeSpillsUsingStandardWriter(spills);
      }
    } else {
      partitionLengths = mergeSpillsUsingStandardWriter(spills);
    }
    return partitionLengths;
  }

  private long[] mergeSpillsUsingStandardWriter(SpillInfo[] spills) throws IOException {
    long[] partitionLengths;
    final boolean fastMergeEnabled =
        (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FAST_MERGE_ENABLE());
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    final ShuffleMapOutputWriter mapWriter =
        shuffleExecutorComponents.createMapOutputWriter(
            shuffleId, mapId, partitioner.numPartitions());
    try {
      // There are multiple spills to merge, so none of these spill files' lengths were counted
      // towards our shuffle write count or shuffle write time. If we use the slow merge path,
      // then the final output file's size won't necessarily be equal to the sum of the spill
      // files' sizes. To guard against this case, we look at the output file's actual size when
      // computing shuffle bytes written.
      //
      // We allow the individual merge methods to report their own IO times since different merge
      // strategies use different IO techniques.  We count IO during merge towards the shuffle
      // write time, which appears to be consistent with the "not bypassing merge-sort" branch in
      // ExternalSorter.
      if (fastMergeEnabled) {
        // Comet native shuffle writer uses its compression codec instead of Spark's. So different
        // Spark where fast spill merge is only supported when compression is disabled or when
        // using a compression codec that supports concatenation of compressed streams, we can
        // perform a fast spill merge that doesn't need to interpret the spilled bytes.
        if (transferToEnabled && !encryptionEnabled) {
          logger.debug("Using transferTo-based fast merge");
          mergeSpillsWithTransferTo(spills, mapWriter);
        } else {
          logger.debug("Using fileStream-based fast merge");
          mergeSpillsWithFileStream(spills, mapWriter);
        }
      } else {
        logger.debug("Using slow merge");
        mergeSpillsWithFileStream(spills, mapWriter);
      }
      // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
      // in-memory records, we write out the in-memory records to a file but do not count that
      // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
      // to be counted as shuffle write, but this will lead to double-counting of the final
      // SpillInfo's bytes.
      writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
      partitionLengths = mapWriter.commitAllPartitions(sorter.getChecksums()).getPartitionLengths();
    } catch (Exception e) {
      try {
        mapWriter.abort(e);
      } catch (Exception e2) {
        logger.warn("Failed to abort writing the map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
    return partitionLengths;
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than the
   * NIO-based merge, {@link CometUnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[],
   * ShuffleMapOutputWriter)}, and it's mostly used in cases where the IO compression codec does not
   * support concatenation of compressed data, when encryption is enabled, or when users have
   * explicitly disabled use of {@code transferTo} in order to work around kernel bugs. This code
   * path might also be faster in cases where individual partition size in a spill is small and
   * UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small disk ios which is
   * inefficient. In those case, Using large buffers for input and output files helps reducing the
   * number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param mapWriter the map output writer to use for output.
   * @return the partition lengths in the merged file.
   */
  private void mergeSpillsWithFileStream(SpillInfo[] spills, ShuffleMapOutputWriter mapWriter)
      throws IOException {
    logger.debug("Merge shuffle spills with FileStream for mapId {}", mapId);
    final int numPartitions = partitioner.numPartitions();
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] =
            new NioBufferedFileInputStream(spills[i].file, inputBufferSizeInBytes);
        // Only convert the partitionLengths when debug level is enabled.
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Partition lengths for mapId {} in Spill {}: {}",
              mapId,
              i,
              Arrays.toString(spills[i].partitionLengths));
        }
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        boolean copyThrewException = true;
        ShufflePartitionWriter writer = mapWriter.getPartitionWriter(partition);
        OutputStream partitionOutput = writer.openStream();
        try {
          partitionOutput = new TimeTrackingOutputStream(writeMetrics, partitionOutput);
          partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
          for (int i = 0; i < spills.length; i++) {
            final long partitionLengthInSpill = spills[i].partitionLengths[partition];

            if (partitionLengthInSpill > 0) {
              InputStream partitionInputStream = null;
              boolean copySpillThrewException = true;
              try {
                partitionInputStream =
                    new LimitedInputStream(spillInputStreams[i], partitionLengthInSpill, false);
                ByteStreams.copy(partitionInputStream, partitionOutput);
                copySpillThrewException = false;
              } finally {
                Closeables.close(partitionInputStream, copySpillThrewException);
              }
            }
          }
          copyThrewException = false;
        } finally {
          Closeables.close(partitionOutput, copyThrewException);
        }
        long numBytesWritten = writer.getNumBytesWritten();
        writeMetrics.incBytesWritten(numBytesWritten);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
    }
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes. This is
   * only safe when the IO compression codec and serializer support concatenation of serialized
   * streams.
   *
   * @param spills the spills to merge.
   * @param mapWriter the map output writer to use for output.
   * @return the partition lengths in the merged file.
   */
  private void mergeSpillsWithTransferTo(SpillInfo[] spills, ShuffleMapOutputWriter mapWriter)
      throws IOException {
    logger.debug("Merge shuffle spills with TransferTo for mapId {}", mapId);
    final int numPartitions = partitioner.numPartitions();
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
        // Only convert the partitionLengths when debug level is enabled.
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Partition lengths for mapId {} in Spill {}: {}",
              mapId,
              i,
              Arrays.toString(spills[i].partitionLengths));
        }
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        boolean copyThrewException = true;
        ShufflePartitionWriter writer = mapWriter.getPartitionWriter(partition);
        WritableByteChannelWrapper resolvedChannel =
            writer
                .openChannelWrapper()
                .orElseGet(() -> new StreamFallbackChannelWrapper(openStreamUnchecked(writer)));
        try {
          for (int i = 0; i < spills.length; i++) {
            long partitionLengthInSpill = spills[i].partitionLengths[partition];
            final FileChannel spillInputChannel = spillInputChannels[i];
            final long writeStartTime = System.nanoTime();
            Utils.copyFileStreamNIO(
                spillInputChannel,
                resolvedChannel.channel(),
                spillInputChannelPositions[i],
                partitionLengthInSpill);
            copyThrewException = false;
            spillInputChannelPositions[i] += partitionLengthInSpill;
            writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          }
        } finally {
          Closeables.close(resolvedChannel, copyThrewException);
        }
        long numBytes = writer.getNumBytesWritten();
        writeMetrics.incBytesWritten(numBytes);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        assert (spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }

  @Override
  public long[] getPartitionLengths() {
    return new long[0];
  }

  private static final class StreamFallbackChannelWrapper implements WritableByteChannelWrapper {
    private final WritableByteChannel channel;

    StreamFallbackChannelWrapper(OutputStream fallbackStream) {
      this.channel = Channels.newChannel(fallbackStream);
    }

    @Override
    public WritableByteChannel channel() {
      return channel;
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}

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

package org.apache.comet.parquet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkEnv;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Comet specific Parquet related read options.
 *
 * <p>TODO: merge this with {@link org.apache.parquet.HadoopReadOptions} once PARQUET-2203 is done.
 */
public class ReadOptions {
  private static final Logger LOG = LoggerFactory.getLogger(ReadOptions.class);
  public static final String COMET_PARQUET_PARALLEL_IO_ENABLED =
      "comet.parquet.read.parallel.io.enabled";
  public static final boolean COMET_PARQUET_PARALLEL_IO_ENABLED_DEFAULT = true;

  public static final String COMET_PARQUET_PARALLEL_IO_THREADS =
      "comet.parquet.read.parallel.io.thread-pool.size";
  public static final int COMET_PARQUET_PARALLEL_IO_THREADS_DEFAULT = 32;

  public static final String COMET_IO_MERGE_RANGES = "comet.parquet.read.io.mergeRanges";
  private static final boolean COMET_IO_MERGE_RANGES_DEFAULT = true;

  public static final String COMET_IO_MERGE_RANGES_DELTA =
      "comet.parquet.read.io.mergeRanges.delta";
  private static final int COMET_IO_MERGE_RANGES_DELTA_DEFAULT = 1 << 23; // 8 MB

  // Max number of concurrent tasks we expect. Used to autoconfigure S3 client connections
  public static final int S3A_MAX_EXPECTED_PARALLELISM = 32;
  // defined in hadoop-aws - org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS
  public static final String S3A_MAXIMUM_CONNECTIONS = "fs.s3a.connection.maximum";
  // default max connections in S3A - org.apache.hadoop.fs.s3a.Constants.DEFAULT_MAXIMUM_CONNECTIONS
  public static final int S3A_DEFAULT_MAX_HTTP_CONNECTIONS = 96;

  public static final String S3A_READAHEAD_RANGE = "fs.s3a.readahead.range";
  // Default read ahead range in Hadoop is 64K; we increase it to 1 MB
  public static final long COMET_DEFAULT_READAHEAD_RANGE = 1 * 1024 * 1024; // 1 MB

  private final boolean parallelIOEnabled;
  private final int parallelIOThreadPoolSize;
  private final boolean ioMergeRanges;
  private final int ioMergeRangesDelta;

  ReadOptions(
      boolean parallelIOEnabled,
      int parallelIOThreadPoolSize,
      boolean ioMergeRanges,
      int ioMergeRangesDelta) {
    this.parallelIOEnabled = parallelIOEnabled;
    this.parallelIOThreadPoolSize = parallelIOThreadPoolSize;
    this.ioMergeRanges = ioMergeRanges;
    this.ioMergeRangesDelta = ioMergeRangesDelta;
  }

  public boolean isParallelIOEnabled() {
    return this.parallelIOEnabled;
  }

  public int parallelIOThreadPoolSize() {
    return this.parallelIOThreadPoolSize;
  }

  public boolean isIOMergeRangesEnabled() {
    return ioMergeRanges;
  }

  public int getIOMergeRangesDelta() {
    return ioMergeRangesDelta;
  }

  public static Builder builder(Configuration conf) {
    return new Builder(conf);
  }

  public static Builder builder() {
    return builder(new Configuration());
  }

  public static class Builder {
    private final Configuration conf;

    private boolean parallelIOEnabled;
    private int parallelIOThreadPoolSize;
    private boolean ioMergeRanges;
    private int ioMergeRangesDelta;

    /**
     * Whether to enable Parquet parallel IO when reading row groups. If true, Parquet reader will
     * use multiple threads to read multiple chunks of data from the current row group in parallel.
     */
    public Builder enableParallelIO(boolean b) {
      this.parallelIOEnabled = b;
      return this;
    }

    /**
     * Specify the number of threads to be used in parallel IO.
     *
     * <p><b>Note</b>: this will only be effective if parallel IO is enabled (e.g., via {@link
     * #enableParallelIO(boolean)}).
     */
    public Builder withParallelIOThreadPoolSize(int numThreads) {
      this.parallelIOThreadPoolSize = numThreads;
      return this;
    }

    public Builder enableIOMergeRanges(boolean enableIOMergeRanges) {
      this.ioMergeRanges = enableIOMergeRanges;
      return this;
    }

    public Builder withIOMergeRangesDelta(int ioMergeRangesDelta) {
      this.ioMergeRangesDelta = ioMergeRangesDelta;
      return this;
    }

    public ReadOptions build() {
      return new ReadOptions(
          parallelIOEnabled, parallelIOThreadPoolSize, ioMergeRanges, ioMergeRangesDelta);
    }

    public Builder(Configuration conf) {
      this.conf = conf;
      this.parallelIOEnabled =
          conf.getBoolean(
              COMET_PARQUET_PARALLEL_IO_ENABLED, COMET_PARQUET_PARALLEL_IO_ENABLED_DEFAULT);
      this.parallelIOThreadPoolSize =
          conf.getInt(COMET_PARQUET_PARALLEL_IO_THREADS, COMET_PARQUET_PARALLEL_IO_THREADS_DEFAULT);
      this.ioMergeRanges = conf.getBoolean(COMET_IO_MERGE_RANGES, COMET_IO_MERGE_RANGES_DEFAULT);
      this.ioMergeRangesDelta =
          conf.getInt(COMET_IO_MERGE_RANGES_DELTA, COMET_IO_MERGE_RANGES_DELTA_DEFAULT);
      // override some S3 defaults
      setS3Config();
    }

    // For paths to S3, if the s3 connection pool max is less than twice the product of
    // parallel reader threads * number of cores, then increase the connection pool max
    private void setS3Config() {
      int s3ConnectionsMax = S3A_DEFAULT_MAX_HTTP_CONNECTIONS;
      SparkEnv env = SparkEnv.get();
      // Use a default number of cores in case we are using the FileReader outside the context
      // of Spark.
      int numExecutorCores = S3A_MAX_EXPECTED_PARALLELISM;
      if (env != null) {
        numExecutorCores = env.conf().getInt(SparkLauncher.EXECUTOR_CORES, numExecutorCores);
      }
      int parallelReaderThreads = this.parallelIOEnabled ? this.parallelIOThreadPoolSize : 1;
      s3ConnectionsMax = Math.max(numExecutorCores * parallelReaderThreads * 2, s3ConnectionsMax);

      setS3ConfIfGreater(conf, S3A_MAXIMUM_CONNECTIONS, s3ConnectionsMax);
      setS3ConfIfGreater(conf, S3A_READAHEAD_RANGE, COMET_DEFAULT_READAHEAD_RANGE);
    }

    // Update the conf iff the new value is greater than the existing val
    private void setS3ConfIfGreater(Configuration conf, String key, int newVal) {
      int maxVal = newVal;
      String curr = conf.get(key);
      if (curr != null && !curr.isEmpty()) {
        maxVal = Math.max(Integer.parseInt(curr), newVal);
      }
      LOG.info("File reader auto configured '{}={}'", key, maxVal);
      conf.set(key, Integer.toString(maxVal));
    }

    // Update the conf iff the new value is greater than the existing val. This handles values that
    // may have suffixes (K, M, G, T, P, E) indicating well known bytes size suffixes
    private void setS3ConfIfGreater(Configuration conf, String key, long newVal) {
      long maxVal = conf.getLongBytes(key, newVal);
      maxVal = Math.max(maxVal, newVal);
      LOG.info("File reader auto configured '{}={}'", key, maxVal);
      conf.set(key, Long.toString(maxVal));
    }
  }
}

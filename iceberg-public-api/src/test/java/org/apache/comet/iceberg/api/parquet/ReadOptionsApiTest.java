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

package org.apache.comet.iceberg.api.parquet;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import org.apache.comet.iceberg.api.AbstractApiTest;
import org.apache.comet.parquet.ReadOptions;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the ReadOptions public API. */
public class ReadOptionsApiTest extends AbstractApiTest {

  @Test
  public void testBuilderCreation() {
    Configuration conf = new Configuration();
    ReadOptions.Builder builder = ReadOptions.builder(conf);
    assertThat(builder).isNotNull();
  }

  @Test
  public void testBuildDefaultOptions() {
    Configuration conf = new Configuration();
    ReadOptions options = ReadOptions.builder(conf).build();

    assertThat(options).isNotNull();
  }

  @Test
  public void testBuilderWithParallelIO() {
    Configuration conf = new Configuration();
    ReadOptions options =
        ReadOptions.builder(conf).enableParallelIO(true).withParallelIOThreadPoolSize(8).build();

    assertThat(options).isNotNull();
    assertThat(options.isParallelIOEnabled()).isTrue();
    assertThat(options.parallelIOThreadPoolSize()).isEqualTo(8);
  }

  @Test
  public void testBuilderWithIOMergeRanges() {
    Configuration conf = new Configuration();
    ReadOptions options =
        ReadOptions.builder(conf)
            .enableIOMergeRanges(true)
            .withIOMergeRangesDelta(1024 * 1024)
            .build();

    assertThat(options).isNotNull();
    assertThat(options.isIOMergeRangesEnabled()).isTrue();
    assertThat(options.getIOMergeRangesDelta()).isEqualTo(1024 * 1024);
  }

  @Test
  public void testBuilderWithAdjustReadRangeSkew() {
    Configuration conf = new Configuration();
    ReadOptions options = ReadOptions.builder(conf).adjustReadRangeSkew(true).build();

    assertThat(options).isNotNull();
    assertThat(options.adjustReadRangesSkew()).isTrue();
  }

  @Test
  public void testBuilderChaining() {
    Configuration conf = new Configuration();
    ReadOptions options =
        ReadOptions.builder(conf)
            .enableParallelIO(true)
            .withParallelIOThreadPoolSize(4)
            .enableIOMergeRanges(true)
            .withIOMergeRangesDelta(512 * 1024)
            .adjustReadRangeSkew(false)
            .build();

    assertThat(options).isNotNull();
    assertThat(options.isParallelIOEnabled()).isTrue();
    assertThat(options.parallelIOThreadPoolSize()).isEqualTo(4);
    assertThat(options.isIOMergeRangesEnabled()).isTrue();
    assertThat(options.getIOMergeRangesDelta()).isEqualTo(512 * 1024);
    assertThat(options.adjustReadRangesSkew()).isFalse();
  }

  @Test
  public void testBuilderWithDisabledOptions() {
    Configuration conf = new Configuration();
    ReadOptions options =
        ReadOptions.builder(conf).enableParallelIO(false).enableIOMergeRanges(false).build();

    assertThat(options).isNotNull();
    assertThat(options.isParallelIOEnabled()).isFalse();
    assertThat(options.isIOMergeRangesEnabled()).isFalse();
  }

  @Test
  public void testS3ConfigConstants() {
    // Verify S3-related constants are accessible
    assertThat(ReadOptions.S3A_MAX_EXPECTED_PARALLELISM).isEqualTo(32);
    assertThat(ReadOptions.S3A_MAXIMUM_CONNECTIONS).isEqualTo("fs.s3a.connection.maximum");
    assertThat(ReadOptions.S3A_DEFAULT_MAX_HTTP_CONNECTIONS).isEqualTo(96);
    assertThat(ReadOptions.S3A_READAHEAD_RANGE).isEqualTo("fs.s3a.readahead.range");
    assertThat(ReadOptions.COMET_DEFAULT_READAHEAD_RANGE).isEqualTo(1024 * 1024);
  }
}

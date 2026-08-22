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

import java.lang.reflect.Method;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Regression guard for {@link CometFileKeyUnwrapper#normalizeS3Scheme}. The put side is called with
 * the user-facing URI (e.g. {@code blob://...}), while the native side JNIs back with the URI after
 * Comet has rewritten it to {@code s3://} in {@code prepare_object_store_with_configs}. Both sides
 * must normalize to the SAME canonical form ({@code s3a://}) or encrypted Parquet reads over {@code
 * blob://} tables fail with {@code Failed to find DecryptionKeyRetriever}.
 */
public class TestCometFileKeyUnwrapper {

  private static String normalize(String filePath) throws Exception {
    Method m = CometFileKeyUnwrapper.class.getDeclaredMethod("normalizeS3Scheme", String.class);
    m.setAccessible(true);
    return (String) m.invoke(new CometFileKeyUnwrapper(), filePath);
  }

  @Test
  public void allS3AliasesNormalizeToS3a() throws Exception {
    String suffix = "bucket/foo/part-0.parquet";
    String canonical = "s3a://" + suffix;
    assertEquals(canonical, normalize("s3://" + suffix));
    assertEquals(canonical, normalize("s3a://" + suffix));
    assertEquals(canonical, normalize("s3n://" + suffix));
    assertEquals(canonical, normalize("blob://" + suffix));
  }

  @Test
  public void nonS3SchemesPassThrough() throws Exception {
    assertEquals(
        "hdfs://nn/warehouse/part-0.parquet", normalize("hdfs://nn/warehouse/part-0.parquet"));
    assertEquals("file:///tmp/part-0.parquet", normalize("file:///tmp/part-0.parquet"));
  }
}

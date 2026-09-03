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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

/**
 * Regression guard for {@link CometFileKeyUnwrapper#normalizeS3Scheme(String, Set)}. The put side
 * is called with the user-facing URI (e.g. {@code blob://...}), while the native side JNIs back
 * with the URI after Comet has rewritten it to {@code s3://} in {@code
 * prepare_object_store_with_configs}. Both sides must normalize to the SAME canonical form ({@code
 * s3a://}) or encrypted Parquet reads over alias-scheme tables fail with {@code Failed to find
 * DecryptionKeyRetriever}. The alias set is not hardcoded: it comes from {@code
 * fs.comet.s3Compliant.schemes}, so a scheme is only folded when the user opted it in.
 */
public class TestCometFileKeyUnwrapper {

  private static final Set<String> NONE = Collections.emptySet();
  private static final Set<String> BLOB = Collections.singleton("blob");
  private static final Set<String> BLOB_MINIO = new HashSet<>(asList("blob", "minio"));

  private static String norm(String filePath, Set<String> schemes) {
    return CometFileKeyUnwrapper.normalizeS3Scheme(filePath, schemes);
  }

  @Test
  public void baseS3AliasesNormalizeWithoutConfig() {
    // s3/s3a/s3n are always folded, independent of fs.comet.s3Compliant.schemes.
    String suffix = "bucket/foo/part-0.parquet";
    String canonical = "s3a://" + suffix;
    assertEquals(canonical, norm("s3://" + suffix, NONE));
    assertEquals(canonical, norm("s3a://" + suffix, NONE));
    assertEquals(canonical, norm("s3n://" + suffix, NONE));
  }

  @Test
  public void configuredAliasesNormalizeToS3a() {
    // blob and minio fold ONLY when listed in fs.comet.s3Compliant.schemes. This is the case that
    // used to fail: a hardcoded {s3,s3n,s3a,blob} list dropped minio, so an encrypted minio:// read
    // cached under minio://... on the put side but looked up s3a://... on the get side.
    String suffix = "bucket/foo/part-0.parquet";
    String canonical = "s3a://" + suffix;
    assertEquals(canonical, norm("blob://" + suffix, BLOB_MINIO));
    assertEquals(canonical, norm("minio://" + suffix, BLOB_MINIO));
  }

  @Test
  public void unconfiguredAliasPassesThrough() {
    // An alias not opted in is left untouched. Here blob is configured but minio is not.
    String minio = "minio://bucket/foo/part-0.parquet";
    assertEquals(minio, norm(minio, BLOB));
    // With no schemes configured at all, even blob passes through.
    String blob = "blob://bucket/foo/part-0.parquet";
    assertEquals(blob, norm(blob, NONE));
  }

  @Test
  public void aliasSingleAndTripleSlashPromoteBucketToAuthority() {
    // Java's URI.toString() collapses blob:///bucket/key to the single-slash blob:/bucket/key, and
    // some deployments emit the triple-slash form directly. The native rewrite promotes the first
    // path segment into the authority, so the get side reconstructs s3://bucket/key. The put side
    // must canonicalize both forms the same way, otherwise the stored cache key never matches the
    // get side's s3a://bucket/key and reads fail with "Failed to find DecryptionKeyRetriever".
    String canonical = "s3a://bucket/foo/part-0.parquet";
    assertEquals(canonical, norm("blob:/bucket/foo/part-0.parquet", BLOB));
    assertEquals(canonical, norm("blob:///bucket/foo/part-0.parquet", BLOB));
    // The two-slash form and the s3:// the native side calls back with must land on the same key.
    assertEquals(canonical, norm("blob://bucket/foo/part-0.parquet", BLOB));
    assertEquals(canonical, norm("s3://bucket/foo/part-0.parquet", BLOB));
  }

  @Test
  public void nonS3SchemesPassThrough() {
    String hdfs = "hdfs://nn/warehouse/part-0.parquet";
    assertEquals(hdfs, norm(hdfs, BLOB));
    String file = "file:///tmp/part-0.parquet";
    assertEquals(file, norm(file, BLOB));
  }
}

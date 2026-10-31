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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
  public void unlistedSchemesPassThrough() {
    // A scheme not in the opted-in set is left untouched, whether it is a plausible alias (minio,
    // blob) or a non-S3 filesystem. Here blob is configured but minio is not.
    for (String p :
        asList(
            "minio://bucket/foo/part-0.parquet",
            "hdfs://nn/warehouse/part-0.parquet",
            "file:///tmp/part-0.parquet")) {
      assertEquals(p, norm(p, BLOB));
    }
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
  }

  @Test
  public void abfsKeyDropsThePasswordAsNativeDoes() {
    // Native looks keys up by the URL it read the file through, whose ABFS authority has no
    // password and whose scheme the url crate lowercased.
    String host = "acct.dfs.core.windows.net";
    String[][] cases = {
      {"abfss://data:secret@" + host + "/t/r.parquet", "abfss://data@" + host + "/t/r.parquet"},
      {
        "abfs://data:secret@" + host + ":8443/t/r.parquet",
        "abfs://data@" + host + ":8443/t/r.parquet"
      },
      {"abfss://:secret@" + host + "/t/r.parquet", "abfss://" + host + "/t/r.parquet"},
      {"abfss://data@" + host + "/t/r.parquet", "abfss://data@" + host + "/t/r.parquet"},
      {"ABFSS://data:secret@" + host + "/t/r.parquet", "abfss://data@" + host + "/t/r.parquet"},
      {"abfss://data@" + host + "/a@b:c/r.parquet", "abfss://data@" + host + "/a@b:c/r.parquet"},
      {"abfss:/t/r.parquet", "abfss:/t/r.parquet"},
      {"abfss://" + host + "/p", "abfss://" + host + "/p"},
      {"abfss://data:se:cret@" + host + "/p", "abfss://data@" + host + "/p"},
      // The url crate drops the `:` of an empty password.
      {"abfss://data:@" + host + "/p", "abfss://data@" + host + "/p"},
      {"abfss://data:secret@" + host + "?q", "abfss://data@" + host + "?q"},
      {"abfss://data:secret@" + host + "#f", "abfss://data@" + host + "#f"},
      // Only ABFS is rewritten here; S3 user info is left as it is.
      {"s3a://user:secret@bucket/t/r.parquet", "s3a://user:secret@bucket/t/r.parquet"},
      {"hdfs://nn:8020/t/r.parquet", "hdfs://nn:8020/t/r.parquet"},
    };
    for (String[] c : cases) {
      assertEquals(c[0], c[1], CometFileKeyUnwrapper.withoutAbfsPassword(c[0]));
    }
  }

  @Test
  public void abfsKeyStoredWithPasswordIsFoundWithout() {
    Configuration conf = new Configuration(false);
    conf.set(
        DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        FixedKeyFactory.class.getName());
    CometFileKeyUnwrapper unwrapper = new CometFileKeyUnwrapper();
    String stored = "abfss://data:fixture@acct.dfs.core.windows.net/t/r.parquet";
    unwrapper.storeDecryptionKeyRetriever(stored, conf);
    byte[] metadata = new byte[0];
    assertArrayEquals(
        FixedKeyFactory.KEY,
        unwrapper.getKey("abfss://data@acct.dfs.core.windows.net/t/r.parquet", metadata));
    assertArrayEquals(FixedKeyFactory.KEY, unwrapper.getKey(stored, metadata));
  }

  @Test
  public void missingAbfsKeyErrorOmitsThePassword() {
    CometFileKeyUnwrapper unwrapper = new CometFileKeyUnwrapper();
    try {
      unwrapper.getKey("abfss://data:fixture@acct.dfs.core.windows.net/t/r.parquet", new byte[0]);
      fail("expected a missing retriever");
    } catch (ParquetCryptoRuntimeException e) {
      assertEquals(
          "Failed to find DecryptionKeyRetriever for path: "
              + "abfss://data@acct.dfs.core.windows.net/t/r.parquet",
          e.getMessage());
    }
  }

  /** Hands every file a retriever that returns {@link #KEY}. */
  public static class FixedKeyFactory implements DecryptionPropertiesFactory {
    static final byte[] KEY = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration conf, Path path) {
      return FileDecryptionProperties.builder().withKeyRetriever(metadata -> KEY).build();
    }
  }
}

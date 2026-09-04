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
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;

import org.apache.comet.CometConf;

// spotless:off
/*
 * Architecture Overview:
 *
 *          JVM Side                           |                     Native Side
 *   ┌─────────────────────────────────────┐   |   ┌─────────────────────────────────────┐
 *   │     CometFileKeyUnwrapper           │   |   │       Parquet File Reading          │
 *   │                                     │   |   │                                     │
 *   │  ┌─────────────────────────────┐    │   |   │  ┌─────────────────────────────┐    │
 *   │  │      hadoopConf             │    │   |   │  │     file1.parquet           │    │
 *   │  │   (Configuration)           │    │   |   │  │     file2.parquet           │    │
 *   │  └─────────────────────────────┘    │   |   │  │     file3.parquet           │    │
 *   │              │                      │   |   │  └─────────────────────────────┘    │
 *   │              ▼                      │   |   │              │                      │
 *   │  ┌─────────────────────────────┐    │   |   │              │                      │
 *   │  │      factoryCache           │    │   |   │              ▼                      │
 *   │  │   (many-to-one mapping)     │    │   |   │  ┌─────────────────────────────┐    │
 *   │  │                             │    │   |   │  │  Parse file metadata &      │    │
 *   │  │ file1 ──┐                   │    │   |   │  │  extract keyMetadata        │    │
 *   │  │ file2 ──┼─► DecryptionProps │    │   |   │  └─────────────────────────────┘    │
 *   │  │ file3 ──┘      Factory      │    │   |   │              │                      │
 *   │  └─────────────────────────────┘    │   |   │              │                      │
 *   │              │                      │   |   │              ▼                      │
 *   │              ▼                      │   |   │  ╔═════════════════════════════╗    │
 *   │  ┌─────────────────────────────┐    │   |   │  ║        JNI CALL:            ║    │
 *   │  │      retrieverCache         │    │   |   │  ║       getKey(filePath,      ║    │
 *   │  │  filePath -> KeyRetriever   │◄───┼───┼───┼──║        keyMetadata)         ║    │
 *   │  └─────────────────────────────┘    │   |   │  ╚═════════════════════════════╝    │
 *   │              │                      │   |   │                                     │
 *   │              ▼                      │   |   │                                     │
 *   │  ┌─────────────────────────────┐    │   |   │                                     │
 *   │  │  DecryptionKeyRetriever     │    │   |   │                                     │
 *   │  │     .getKey(keyMetadata)    │    │   |   │                                     │
 *   │  └─────────────────────────────┘    │   |   │                                     │
 *   │              │                      │   |   │                                     │
 *   │              ▼                      │   |   │                                     │
 *   │  ┌─────────────────────────────┐    │   |   │  ┌─────────────────────────────┐    │
 *   │  │      return key bytes       │────┼───┼───┼─►│   Use key for decryption    │    │
 *   │  └─────────────────────────────┘    │   |   │  │    of parquet data          │    │
 *   └─────────────────────────────────────┘   |   │  └─────────────────────────────┘    │
 *                                             |   └─────────────────────────────────────┘
 *                                             |
 *                                    JNI Boundary
 *
 * Setup Phase (storeDecryptionKeyRetriever):
 * 1. hadoopConf → DecryptionPropertiesFactory (cached in factoryCache)
 * 2. Factory + filePath → DecryptionKeyRetriever (cached in retrieverCache)
 *
 * Runtime Phase (getKey):
 * 3. Native code calls getKey(filePath, keyMetadata) ──► JVM
 * 4. Retrieve cached DecryptionKeyRetriever for filePath
 * 5. KeyRetriever.getKey(keyMetadata) → decrypted key bytes
 * 6. Return key bytes ──► Native code for parquet decryption
 */
// spotless:on

/**
 * Helper class to access DecryptionKeyRetriever.getKey from native code via JNI. This class handles
 * the complexity of creating and caching properly configured DecryptionKeyRetriever instances using
 * DecryptionPropertiesFactory. The life of this object is meant to map to a single Comet plan, so
 * associated with CometExecIterator.
 */
public class CometFileKeyUnwrapper {

  // Hadoop config key listing the user-opted-in S3-compliant alias schemes (e.g. blob, minio),
  // comma-separated and case-insensitive. Resolved from the Hadoop conf on the put side, so it must
  // be set at session-creation time.
  private static final String S3_COMPLIANT_SCHEMES_KEY = CometConf.COMET_S3_COMPLIANT_SCHEMES_KEY();

  // Schemes object_store always treats as aliases of s3://, independent of config. The native side
  // rewrites every alias (these plus the configured ones below) to s3:// before it JNIs getKey
  // back, so the get side always sees one of these.
  private static final Set<String> BASE_S3_ALIAS_SCHEMES = Set.of("s3", "s3n", "s3a");

  // User-opted-in S3-compliant alias schemes from S3_COMPLIANT_SCHEMES_KEY, resolved from the
  // Hadoop conf on the put side and cached so the get side folds the same aliases to one cache key.
  // volatile because getKey may be invoked from a native callback thread after the put side (on the
  // setup thread) populated it. Empty until the first storeDecryptionKeyRetriever, which precedes
  // any getKey.
  private volatile Set<String> s3CompliantSchemes = Collections.emptySet();

  // Each file path gets a unique DecryptionKeyRetriever
  private final ConcurrentHashMap<String, DecryptionKeyRetriever> retrieverCache =
      new ConcurrentHashMap<>();

  // Cache the factory since we should be using the same hadoopConf for every file in this scan.
  private DecryptionPropertiesFactory factory = null;
  // Cache the hadoopConf just to assert the assumption above.
  private Configuration conf = null;

  /**
   * Normalizes S3 and S3-compliant alias URI schemes to a canonical {@code s3a://<bucket>/<key>}
   * form so cache lookups agree regardless of the scheme used. S3 can be addressed via {@code
   * s3://}, {@code s3a://}, {@code s3n://}, and any scheme the user opted into via {@code
   * fs.comet.s3Compliant.schemes} (e.g. {@code blob://}). The put and get sides must agree, because
   * the put side is called with the user-facing scheme (e.g. blob://) while the native side JNIs
   * back with the scheme after {@code prepare_object_store_with_configs} has already rewritten
   * aliases to s3://.
   *
   * <p>The native rewrite also promotes the first path segment into the authority for the
   * single-slash {@code blob:/bucket/key} (Java opaque form) and triple-slash {@code
   * blob:///bucket/key} (empty authority) shapes, so the get side reconstructs {@code
   * s3://bucket/key}. This method applies the same promotion, otherwise a {@code blob:/...} or
   * {@code blob:///...} input listed by Spark would be cached under a key that never matches the
   * get side's {@code s3a://bucket/key}.
   *
   * @param filePath The file path that may contain an S3 or alias URI
   * @return The file path with a normalized {@code s3a://} scheme and bucket in the authority
   */
  private String normalizeS3Scheme(final String filePath) {
    return normalizeS3Scheme(filePath, s3CompliantSchemes);
  }

  /**
   * Scheme-normalization core, parameterized by the opted-in alias schemes so it can be unit-tested
   * without a live Hadoop conf. Folds {@code s3}/{@code s3n}/{@code s3a} (always) and any {@code
   * s3CompliantSchemes} entry (case-insensitive) to canonical {@code s3a://<bucket>/<key>}; every
   * other scheme is returned unchanged.
   */
  static String normalizeS3Scheme(final String filePath, final Set<String> s3CompliantSchemes) {
    final int schemeEnd = filePath.indexOf(':');
    if (schemeEnd <= 0) {
      // Bare path with no scheme -- nothing to canonicalize.
      return filePath;
    }
    final String scheme = filePath.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
    if (!BASE_S3_ALIAS_SCHEMES.contains(scheme) && !s3CompliantSchemes.contains(scheme)) {
      return filePath;
    }
    // Strip the scheme and every leading slash, re-join under s3a:// to promote the first path
    // segment into the authority for the single-/triple-slash forms (matches the native
    // rewrite_alias_to_s3 behavior).
    return "s3a://" + StringUtils.stripStart(filePath.substring(schemeEnd + 1), "/");
  }

  /**
   * Reads the opted-in alias schemes from {@code fs.comet.s3Compliant.schemes} as a lowercased set.
   * Hadoop's own comma-splitting is reused so the value parses exactly as {@code
   * NativeConfig.parseSchemeSet} does on the Scala side.
   */
  private static Set<String> readS3CompliantSchemes(final Configuration hadoopConf) {
    final Set<String> schemes = new HashSet<>();
    for (String s : hadoopConf.getTrimmedStringCollection(S3_COMPLIANT_SCHEMES_KEY)) {
      schemes.add(s.toLowerCase(Locale.ROOT));
    }
    return schemes;
  }

  /**
   * Creates and stores a DecryptionKeyRetriever instance for the given file path.
   *
   * @param filePath The path to the Parquet file
   * @param hadoopConf The Hadoop Configuration to use for this file path
   */
  public void storeDecryptionKeyRetriever(final String filePath, final Configuration hadoopConf) {
    // Use DecryptionPropertiesFactory.loadFactory to get the factory and then call
    // getFileDecryptionProperties
    if (factory == null) {
      factory = DecryptionPropertiesFactory.loadFactory(hadoopConf);
      conf = hadoopConf;
      // Resolve the opted-in alias schemes once, before the first normalizeS3Scheme below, so both
      // the put side here and the later getKey side fold the same aliases to one cache key.
      s3CompliantSchemes = readS3CompliantSchemes(hadoopConf);
    } else {
      // Check the assumption that all files have the same hadoopConf and thus same Factory
      assert (conf == hadoopConf);
    }
    final String normalizedPath = normalizeS3Scheme(filePath);
    Path path = new Path(filePath);
    FileDecryptionProperties decryptionProperties =
        factory.getFileDecryptionProperties(hadoopConf, path);

    DecryptionKeyRetriever keyRetriever = decryptionProperties.getKeyRetriever();
    retrieverCache.put(normalizedPath, keyRetriever);
  }

  /**
   * Gets the decryption key for the given key metadata using the cached DecryptionKeyRetriever for
   * the specified file path.
   *
   * @param filePath The path to the Parquet file
   * @param keyMetadata The key metadata bytes from the Parquet file
   * @return The decrypted key bytes
   * @throws ParquetCryptoRuntimeException if key unwrapping fails
   */
  public byte[] getKey(final String filePath, final byte[] keyMetadata)
      throws ParquetCryptoRuntimeException {
    final String normalizedPath = normalizeS3Scheme(filePath);
    DecryptionKeyRetriever keyRetriever = retrieverCache.get(normalizedPath);
    if (keyRetriever == null) {
      throw new ParquetCryptoRuntimeException(
          "Failed to find DecryptionKeyRetriever for path: " + filePath);
    }
    return keyRetriever.getKey(keyMetadata);
  }
}

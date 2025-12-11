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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;

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

  // Each file path gets a unique DecryptionKeyRetriever
  private final ConcurrentHashMap<String, DecryptionKeyRetriever> retrieverCache =
      new ConcurrentHashMap<>();

  // Cache the factory since we should be using the same hadoopConf for every file in this scan.
  private DecryptionPropertiesFactory factory = null;
  // Cache the hadoopConf just to assert the assumption above.
  private Configuration conf = null;

  /**
   * Normalizes S3 URI schemes to a canonical form. S3 can be accessed via multiple schemes (s3://,
   * s3a://, s3n://) that refer to the same logical filesystem. This method ensures consistent cache
   * lookups regardless of which scheme is used.
   *
   * @param filePath The file path that may contain an S3 URI
   * @return The file path with normalized S3 scheme (s3a://)
   */
  private String normalizeS3Scheme(final String filePath) {
    // Normalize s3:// and s3n:// to s3a:// for consistent cache lookups
    // This handles the case where ObjectStoreUrl uses s3:// but Spark uses s3a://
    String s3Prefix = "s3://";
    String s3nPrefix = "s3n://";
    if (filePath.startsWith(s3Prefix)) {
      return "s3a://" + filePath.substring(s3Prefix.length());
    } else if (filePath.startsWith(s3nPrefix)) {
      return "s3a://" + filePath.substring(s3nPrefix.length());
    }
    return filePath;
  }

  /**
   * Creates and stores a DecryptionKeyRetriever instance for the given file path.
   *
   * @param filePath The path to the Parquet file
   * @param hadoopConf The Hadoop Configuration to use for this file path
   */
  public void storeDecryptionKeyRetriever(final String filePath, final Configuration hadoopConf) {
    final String normalizedPath = normalizeS3Scheme(filePath);
    // Use DecryptionPropertiesFactory.loadFactory to get the factory and then call
    // getFileDecryptionProperties
    if (factory == null) {
      factory = DecryptionPropertiesFactory.loadFactory(hadoopConf);
      conf = hadoopConf;
    } else {
      // Check the assumption that all files have the same hadoopConf and thus same Factory
      assert (conf == hadoopConf);
    }
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

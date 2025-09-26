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

  // Each hadoopConf yields a unique DecryptionPropertiesFactory. While it's unlikely that
  // this Comet plan contains more than one hadoopConf, we don't want to assume that. So we'll
  // provide the ability to cache more than one Factory with a map.
  private final ConcurrentHashMap<Configuration, DecryptionPropertiesFactory> factoryCache =
      new ConcurrentHashMap<>();

  /**
   * Creates and stores a DecryptionKeyRetriever instance for the given file path.
   *
   * @param filePath The path to the Parquet file
   * @param hadoopConf The Hadoop Configuration to use for this file path
   */
  public void storeDecryptionKeyRetriever(final String filePath, final Configuration hadoopConf) {
    // Use DecryptionPropertiesFactory.loadFactory to get the factory and then call
    // getFileDecryptionProperties
    DecryptionPropertiesFactory factory = factoryCache.get(hadoopConf);
    if (factory == null) {
      factory = DecryptionPropertiesFactory.loadFactory(hadoopConf);
      factoryCache.put(hadoopConf, factory);
    }
    Path path = new Path(filePath);
    FileDecryptionProperties decryptionProperties =
        factory.getFileDecryptionProperties(hadoopConf, path);

    DecryptionKeyRetriever keyRetriever = decryptionProperties.getKeyRetriever();
    retrieverCache.put(filePath, keyRetriever);
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
    DecryptionKeyRetriever keyRetriever = retrieverCache.get(filePath);
    if (keyRetriever == null) {
      throw new ParquetCryptoRuntimeException(
          "Failed to find DecryptionKeyRetriever for path: " + filePath);
    }
    return keyRetriever.getKey(keyMetadata);
  }
}

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

package org.apache.comet.objectstore

import java.net.URI
import java.util.Locale

import org.apache.hadoop.conf.Configuration

object NativeConfig {

  private val objectStoreConfigPrefixes = Map(
    // Amazon S3 configurations
    "s3" -> Seq("fs.s3a."),
    "s3a" -> Seq("fs.s3a."),
    // Google Cloud Storage configurations
    "gs" -> Seq("fs.gs."),
    // Azure Blob Storage configurations (can use both prefixes)
    "wasb" -> Seq("fs.azure.", "fs.wasb."),
    "wasbs" -> Seq("fs.azure.", "fs.wasb."),
    // Azure Data Lake Storage Gen2 configurations
    "abfs" -> Seq("fs.abfs."),
    // Azure Data Lake Storage Gen2 secure configurations (can use both prefixes)
    "abfss" -> Seq("fs.abfss.", "fs.abfs."))

  /**
   * Extract object store configurations from Hadoop configuration for native DataFusion usage.
   * This includes S3, GCS, Azure and other cloud storage configurations.
   *
   * This method extracts all configurations with supported prefixes, automatically capturing both
   * global configurations (e.g., fs.s3a.access.key) and per-bucket configurations (e.g.,
   * fs.s3a.bucket.{bucket-name}.access.key). The native code will prioritize per-bucket
   * configurations over global ones when both are present.
   *
   * The configurations are passed to the native code which uses object_store's parse_url_opts for
   * consistent and standardized cloud storage support across all providers.
   */
  def extractObjectStoreOptions(hadoopConf: Configuration, uri: URI): Map[String, String] = {
    val scheme = uri.getScheme.toLowerCase(Locale.ROOT)

    // Get prefixes for this scheme, return early if none found
    val prefixes = objectStoreConfigPrefixes.get(scheme)
    if (prefixes.isEmpty) {
      return Map.empty[String, String]
    }

    import scala.collection.JavaConverters._

    // Extract all configurations that match the object store prefixes
    val options = scala.collection.mutable.Map[String, String]()
    hadoopConf.iterator().asScala.foreach { entry =>
      val key = entry.getKey
      val value = entry.getValue
      // Check if key starts with any of the prefixes for this scheme
      if (prefixes.get.exists(prefix => key.startsWith(prefix))) {
        options(key) = value
      }
    }

    options.toMap
  }
}

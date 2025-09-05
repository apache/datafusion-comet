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

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration

import org.apache.comet.CometConf.COMET_LIBHDFS_SCHEMES_KEY

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

    import scala.collection.JavaConverters._
    val options = scala.collection.mutable.Map[String, String]()

    // The schemes will use libhdfs
    val libhdfsSchemes = hadoopConf.get(COMET_LIBHDFS_SCHEMES_KEY)
    if (StringUtils.isNotBlank(libhdfsSchemes)) {
      options(COMET_LIBHDFS_SCHEMES_KEY) = libhdfsSchemes
    }

    // Get prefixes for this scheme, return early if none found
    val prefixes = objectStoreConfigPrefixes.get(scheme)
    if (prefixes.isEmpty) {
      return options.toMap
    }

    // Extract all configurations that match the object store prefixes
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

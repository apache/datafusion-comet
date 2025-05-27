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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.comet.util.Utils

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
  def extractObjectStoreOptions(
      hadoopConf: Configuration,
      uri: URI,
      fileSystemOverride: Option[org.apache.hadoop.fs.FileSystem] = None): Map[String, String] = {
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

    // Add extracted S3 region if available and not already configured. Rust's object_store
    // cannot automatically figure out the region of S3 buckets, and requires region to be correctly
    // configured to work. We extract the region in JVM and pass it to the native code.
    if (Seq("s3", "s3a").contains(scheme)) {
      val bucketName = uri.getHost
      if (!options.contains(s"fs.s3a.bucket.$bucketName.endpoint.region") && !options.contains(
          "fs.s3a.endpoint.region")) {
        getS3BucketRegion(uri, hadoopConf, fileSystemOverride).foreach { region =>
          options(s"fs.s3a.bucket.$bucketName.endpoint.region") = region
        }
      }
    }

    options.toMap
  }

  /**
   * Test-friendly version that allows injecting a FileSystem for mocking.
   */
  private[objectstore] def getS3BucketRegion(
      uri: URI,
      hadoopConf: Configuration,
      fileSystemOverride: Option[org.apache.hadoop.fs.FileSystem] = None): Option[String] = {
    val fs = fileSystemOverride.getOrElse {
      val path = new Path(uri)
      path.getFileSystem(hadoopConf)
    }

    // Use reflection to access S3AFileSystem's getBucketLocation method
    val s3aClass = Utils.classForName("org.apache.hadoop.fs.s3a.S3AFileSystem")
    if (s3aClass.isInstance(fs)) {
      try {
        val getBucketLocationMethod = s3aClass.getDeclaredMethod("getBucketLocation")
        getBucketLocationMethod.setAccessible(true)
        val region = getBucketLocationMethod.invoke(fs).asInstanceOf[String]
        if (region != null && region.nonEmpty && region.trim.nonEmpty) {
          return Some(region)
        }
      } catch {
        case _: Exception =>
        // If reflection fails or method doesn't exist, return None
        // This handles cases where S3AFileSystem API changes or is not available
      }
    }

    None
  }
}

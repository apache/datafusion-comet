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
    // Amazon S3 configurations. `blob` is a Comet-recognized synonym for `s3` and shares the
    // same Hadoop `fs.s3a.*` credential surface (see `prepare_object_store_with_configs`).
    "s3" -> Seq("fs.s3a."),
    "s3a" -> Seq("fs.s3a."),
    "blob" -> Seq("fs.s3a."),
    // Google Cloud Storage configurations
    "gs" -> Seq("fs.gs."),
    // Azure Blob Storage configurations (can use both prefixes)
    "wasb" -> Seq("fs.azure.", "fs.wasb."),
    "wasbs" -> Seq("fs.azure.", "fs.wasb."),
    // Azure Data Lake Storage Gen2 (ABFS) configurations. Hadoop ABFS authentication keys
    "abfs" -> Seq("fs.azure.", "fs.abfs."),
    "abfss" -> Seq("fs.azure.", "fs.abfss.", "fs.abfs."))

  private val blobKeyPattern = "^fs\\.blob\\.([^.]+)\\.(.+)$".r

  // Some blob:// filesystem implementations fall back to the literal string "default" as the
  // authority when the URI has none. For `blob:///bucket/key`, the filesystem therefore looks
  // up `fs.blob.default.*`, while the actual S3 bucket comes from the URL path. Translate
  // `fs.blob.default.*` to the GLOBAL `fs.s3a.*` key (not the per-bucket
  // `fs.s3a.bucket.default.*`) so the credentials/endpoint apply to whichever bucket the URL
  // path resolves to, matching those implementations' semantics.
  private val blobDefaultAuthority = "default"

  /**
   * Translates vendor-style `fs.blob.<authority>.<property>` keys into the `fs.s3a.*` shape that
   * object_store's AmazonS3Builder reads. Some blob:// connectors use per-authority keys and
   * never set a region -- an endpoint alone is enough for the AWS SDK v1 client they build -- and
   * their endpoints are typically path-style against non-AWS services, so an `endpoint` key also
   * enables `path.style.access` on the same scope.
   *
   * `fs.blob.<authority>.*` is the authoritative source for `blob://` URLs, so callers should
   * apply these translations AFTER a plain `fs.s3a.*` pass so blob-supplied values override any
   * unrelated `fs.s3a.*` the user set for a different workload (see 403 misdirect in the class
   * docstring).
   */
  private def translateBlobKeys(hadoopConf: Configuration): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    val out = scala.collection.mutable.Map[String, String]()
    hadoopConf.iterator().asScala.foreach { entry =>
      entry.getKey match {
        case blobKeyPattern(authority, property) =>
          val s3aSuffix = property match {
            case "endpoint" => "endpoint"
            case "awsAccessKeyId" => "access.key"
            case "awsSecretAccessKey" => "secret.key"
            case _ => null
          }
          if (s3aSuffix != null) {
            val scope =
              if (authority == blobDefaultAuthority) "fs.s3a"
              else s"fs.s3a.bucket.$authority"
            out(s"$scope.$s3aSuffix") = entry.getValue
            if (s3aSuffix == "endpoint") {
              out.getOrElseUpdate(s"$scope.path.style.access", "true")
            }
          }
        case _ =>
      }
    }
    out.toMap
  }

  /**
   * Extract object store configurations from Hadoop configuration for native DataFusion usage.
   * This includes S3, GCS, Azure and other cloud storage configurations.
   *
   * This method extracts all configurations with supported prefixes, automatically capturing both
   * global configurations (e.g., fs.s3a.access.key) and per-bucket configurations (e.g.,
   * fs.s3a.bucket.{bucket-name}.access.key). The native code will prioritize per-bucket
   * configurations over global ones when both are present.
   *
   * For `blob://` URIs it also translates vendor-style `fs.blob.<authority>.endpoint`,
   * `fs.blob.<authority>.awsAccessKeyId`, and `fs.blob.<authority>.awsSecretAccessKey` into the
   * equivalent `fs.s3a.bucket.<authority>.endpoint`, `access.key`, and `secret.key`. Because
   * those blob:// backends usually talk path-style to non-AWS endpoints and never set a region,
   * the translation also enables `fs.s3a.bucket.<authority>.path.style.access=true` when a blob
   * endpoint is present.
   *
   * `fs.blob.<authority>.*` is the authoritative source for `blob://` URLs: the user may have
   * `fs.s3a.*` keys targeting a completely different s3a-scheme workload in the same Spark
   * session, and leaking those into blob-scheme connections silently redirects credentials to the
   * wrong service (produces a 403 "The access key Id you provided does not exist in our
   * records"). For `blob://` URIs, blob translations therefore OVERRIDE any conflicting
   * `fs.s3a.*` values the user also set. If you actually want to override a blob endpoint, change
   * the `fs.blob.<authority>.endpoint` value itself.
   *
   * The configurations are passed to the native code which uses object_store's parse_url_opts for
   * consistent and standardized cloud storage support across all providers.
   */
  def extractObjectStoreOptions(hadoopConf: Configuration, uri: URI): Map[String, String] = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("file")

    import scala.jdk.CollectionConverters._
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
      if (prefixes.get.exists(prefix => key.startsWith(prefix))) {
        options(key) = entry.getValue
      }
    }

    // For blob:// URIs, apply vendor-key translation AFTER the fs.s3a.* pass so blob values
    // override any unrelated fs.s3a.* the user set for a different workload.
    if (scheme == "blob") {
      translateBlobKeys(hadoopConf).foreach { case (k, v) => options(k) = v }
    }

    options.toMap
  }
}

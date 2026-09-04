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
import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration

import org.apache.comet.CometConf.{COMET_LIBHDFS_SCHEMES_KEY, COMET_S3_COMPLIANT_SCHEMES_KEY}

object NativeConfig {

  private val objectStoreConfigPrefixes = Map(
    // Amazon S3. Schemes listed in `COMET_S3_COMPLIANT_SCHEMES_KEY` reuse this same `fs.s3a.*`
    // surface, resolved per-URI in `extractObjectStoreOptions`.
    "s3" -> Seq("fs.s3a."),
    "s3a" -> Seq("fs.s3a."),
    // Google Cloud Storage configurations
    "gs" -> Seq("fs.gs."),
    // Azure Blob Storage configurations (can use both prefixes)
    "wasb" -> Seq("fs.azure.", "fs.wasb."),
    "wasbs" -> Seq("fs.azure.", "fs.wasb."),
    // Azure Data Lake Storage Gen2 (ABFS) configurations. Hadoop ABFS authentication keys
    "abfs" -> Seq("fs.azure.", "fs.abfs."),
    "abfss" -> Seq("fs.azure.", "fs.abfss.", "fs.abfs."))

  // Some alias filesystems report the literal authority "default" when the URI has none (e.g.
  // `scheme:///bucket/key`); the real bucket is then promoted from the URL path. Keys under this
  // authority map to the per-bucket `fs.s3a.bucket.<resolved-bucket>.*` scope, NOT global
  // `fs.s3a.*`, because native `get_config` checks per-bucket before global.
  private val vendorDefaultAuthority = "default"

  // Recognized vendor properties -> `fs.s3a` suffix. Mirrors CometIcebergNativeScan's target set.
  // Unknown properties are dropped.
  private val vendorPropertyToS3aSuffix = Map(
    "awsAccessKeyId" -> "access.key",
    "awsSecretAccessKey" -> "secret.key",
    "awsSessionToken" -> "session.token",
    "endpoint" -> "endpoint",
    "region" -> "endpoint.region",
    "pathStyleAccess" -> "path.style.access")

  // Comma-separated scheme list -> trimmed, lowercased set (case-insensitive). Shared with
  // CometScanRule's scheme gate so the JVM admit-decision and native rewrite parse identically.
  private[comet] def parseSchemeSet(raw: String): Set[String] =
    Option(raw)
      .map(_.split(",").iterator.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).toSet)
      .getOrElse(Set.empty)

  // Opt-in S3-compliant alias schemes from the Hadoop config `fs.comet.s3Compliant.schemes`
  // (comma-separated, trimmed, lowercased, empty/missing => none). The native gate no longer
  // claims them, so the opt-in is resolved wherever the Hadoop config is available: the scan gate
  // (CometScanRule) and the Iceberg write path's data-bucket resolution.
  private[comet] def resolveS3CompliantSchemes(hadoopConf: Configuration): Set[String] =
    parseSchemeSet(hadoopConf.get(COMET_S3_COMPLIANT_SCHEMES_KEY))

  // True when the user pinned path-style at this bucket scope. Suppresses the synthesized soft
  // default so an explicit per-bucket setting (including `false`, the escape hatch) survives. A
  // global `fs.s3a.path.style.access` is intentionally ignored: it may target other s3a workloads
  // or be an ambient cluster default. Per-bucket synth wins over global in native anyway.
  private def userSetPathStyle(hadoopConf: Configuration, bucket: Option[String]): Boolean =
    bucket.exists(b => hadoopConf.get(s"fs.s3a.bucket.$b.path.style.access") != null)

  /**
   * The S3 bucket a URI addresses: its authority, or the first path segment for the authorityless
   * `blob:///bucket/key` form (matching the native rewrite that promotes it into the host).
   * Returns None for a non-S3-family scheme, so a local Hadoop-catalog
   * `file:///tmp/warehouse/...` or a `gs://` location yields no bucket rather than a surprising
   * `tmp`: only `s3`/`s3a`/`s3n` and the opted-in `s3CompliantSchemes` share the `fs.s3a.*`
   * per-bucket surface. Callers therefore need no scheme check of their own.
   */
  private[comet] def bucketForUri(uri: URI, s3CompliantSchemes: Set[String]): Option[String] = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    if (!isS3FamilyScheme(scheme, s3CompliantSchemes)) {
      None
    } else {
      Option(uri.getAuthority)
        .map(_.trim)
        .filter(_.nonEmpty)
        .orElse(
          Option(uri.getPath)
            .map(_.stripPrefix("/"))
            .map(_.takeWhile(_ != '/'))
            .filter(_.nonEmpty))
    }
  }

  // s3/s3a/s3n and any opted-in alias share the authorityless path-promotion semantics above.
  private def isS3FamilyScheme(scheme: String, s3CompliantSchemes: Set[String]): Boolean =
    scheme == "s3" || scheme == "s3a" || scheme == "s3n" || s3CompliantSchemes.contains(scheme)

  /**
   * The distinct S3 buckets addressed by the S3-family URIs in `uris` -- `s3`/`s3a`/`s3n` and any
   * opted-in `s3CompliantSchemes` alias -- resolved via [[bucketForUri]], which drops
   * non-S3-family URIs (`file`, `hdfs`, `gs`, `oss`, ...) since their authority is not an S3
   * bucket and they do not share the `fs.s3a.*` per-bucket surface. Used to detect a scan whose
   * files span multiple buckets that a single native object store, keyed on one bucket, cannot
   * serve.
   */
  private[comet] def s3FamilyBuckets(
      uris: Seq[URI],
      s3CompliantSchemes: Set[String]): Set[String] =
    uris.iterator.flatMap(bucketForUri(_, s3CompliantSchemes)).toSet

  /**
   * Translate vendor-style `fs.<scheme>.<authority>.<property>` keys into the `fs.s3a.*` shape
   * object_store's AmazonS3Builder reads, for a configured S3-compliant `scheme`. Recognized
   * properties are `vendorPropertyToS3aSuffix`; unknown ones are dropped.
   *
   * Keys land at the per-bucket `fs.s3a.bucket.<bucket>.*` scope (for the `default` authority the
   * bucket is `defaultBucket`, promoted from the URL path) -- the scope native `get_config`
   * checks first. The authority is matched greedily so dotted bucket names survive.
   *
   * An `endpoint` also synthesizes `path.style.access=true` as a soft default, suppressed by an
   * explicit per-bucket setting or vendor `pathStyleAccess`. Applied AFTER the plain `fs.s3a.*`
   * pass so real vendor keys override conflicting `fs.s3a.*` (the 403-misdirect note below).
   *
   * `confEntries` is the caller's one-time snapshot of the Hadoop config (see
   * `extractObjectStoreOptions`); `hadoopConf` is still used for `${...}` substitution and
   * targeted per-bucket lookups.
   */
  private def translateVendorKeys(
      confEntries: Seq[(String, String)],
      hadoopConf: Configuration,
      scheme: String,
      defaultBucket: Option[String]): Map[String, String] = {
    // `fs.<scheme>.<authority>.<property>`; scheme is regex-quoted (may contain `.`/`+`/`-`).
    val vendorKeyPattern = ("^fs\\." + Pattern.quote(scheme) + "\\.(.+)\\.([^.]+)$").r
    val out = scala.collection.mutable.Map[String, String]()

    // Apply `default`-authority keys BEFORE explicit ones. A `default` key (promoted to the
    // URL-path bucket) and an explicit `fs.<scheme>.<bucket>.*` can resolve to the same
    // `fs.s3a.bucket.<bucket>.*` scope, and the explicit spelling must win -- which it does only
    // when written last. Otherwise the winner is whichever config entry is yielded last (hash
    // order), so the same config could pick either endpoint from one run to the next.
    val matched = confEntries.flatMap { case (key, value) =>
      key match {
        case vendorKeyPattern(authority, property) =>
          vendorPropertyToS3aSuffix
            .get(property)
            .map(suffix => (authority, suffix, substitutedValue(hadoopConf, key, value)))
        case _ => None
      }
    }
    val (defaults, explicit) = matched.partition(_._1 == vendorDefaultAuthority)

    (defaults ++ explicit).foreach { case (authority, suffix, value) =>
      val bucket =
        if (authority == vendorDefaultAuthority) defaultBucket else Some(authority)
      val scope = bucket.map(b => s"fs.s3a.bucket.$b").getOrElse("fs.s3a")
      out(s"$scope.$suffix") = value
      if (suffix == "endpoint" && !userSetPathStyle(hadoopConf, bucket)) {
        out.getOrElseUpdate(s"$scope.path.style.access", "true")
      }
    }
    out.toMap
  }

  /**
   * Extract object store configs (S3, GCS, Azure, ...) from the Hadoop configuration for native
   * DataFusion. Captures global and per-bucket keys; native code prefers per-bucket.
   *
   * A scheme listed in `COMET_S3_COMPLIANT_SCHEMES_KEY` reuses the `fs.s3a.*` surface, and its
   * vendor `fs.<scheme>.<authority>.*` keys are translated to `fs.s3a.*` (see
   * `translateVendorKeys`) AFTER the `fs.s3a.*` pass, so real vendor values override conflicting
   * `fs.s3a.*` that would otherwise misdirect credentials to a 403. Empty by default: no alias
   * scheme unless opted in.
   *
   * The result feeds object_store's parse_url_opts natively.
   */
  def extractObjectStoreOptions(hadoopConf: Configuration, uri: URI): Map[String, String] = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("file")

    import scala.jdk.CollectionConverters._
    val options = scala.collection.mutable.Map[String, String]()

    // Scheme lists consumed on the native side ride along in the options map (both empty by
    // default). Copy through when set.
    val libhdfsSchemes = hadoopConf.get(COMET_LIBHDFS_SCHEMES_KEY)
    if (StringUtils.isNotBlank(libhdfsSchemes)) {
      options(COMET_LIBHDFS_SCHEMES_KEY) = libhdfsSchemes
    }
    val s3CompliantRaw = hadoopConf.get(COMET_S3_COMPLIANT_SCHEMES_KEY)
    if (StringUtils.isNotBlank(s3CompliantRaw)) {
      options(COMET_S3_COMPLIANT_SCHEMES_KEY) = s3CompliantRaw
    }
    val s3CompliantSchemes = parseSchemeSet(s3CompliantRaw)

    // Prefixes for this scheme; a configured S3-compliant alias reuses the fs.s3a.* surface.
    val prefixes = objectStoreConfigPrefixes.get(scheme).orElse {
      if (s3CompliantSchemes.contains(scheme)) objectStoreConfigPrefixes.get("s3a") else None
    }
    if (prefixes.isEmpty) {
      return options.toMap
    }

    // Materialize the Hadoop config once: `iterator()` rebuilds a full copy of the property map on
    // every call, so we reuse this snapshot for the prefix pass and the vendor translation below.
    val entries = hadoopConf.iterator().asScala.map(e => e.getKey -> e.getValue).toVector

    // Extract all configurations that match the object store prefixes
    entries.foreach { case (key, value) =>
      if (prefixes.get.exists(prefix => key.startsWith(prefix))) {
        options(key) = substitutedValue(hadoopConf, key, value)
      }
    }

    // Configured S3-compliant scheme: translate vendor keys AFTER the fs.s3a.* pass so real vendor
    // values override conflicting fs.s3a.*. Pass the resolved bucket so `fs.<scheme>.default.*`
    // lands at that bucket's scope.
    if (s3CompliantSchemes.contains(scheme)) {
      translateVendorKeys(entries, hadoopConf, scheme, bucketForUri(uri, s3CompliantSchemes))
        .foreach { case (k, v) => options(k) = v }
    }

    options.toMap
  }

  /**
   * The value Hadoop's own consumers observe for `key`. `Configuration#get` expands any `${...}`
   * variable reference in the stored value against other conf entries and system properties,
   * while `Configuration.Entry#getValue` (what `iterator()` surfaces) is the raw, unexpanded
   * literal. Forwarding the raw literal here would diverge from every Hadoop-side consumer
   * whenever a value contains such a reference. Substitution is bounded (Hadoop caps recursion at
   * `MAX_SUBST`, currently 20 passes) and purely in-memory, so resolving it here is cheap and has
   * no side effects.
   *
   * Falls back to `rawValue` when `get` returns `null` (deprecated-key aliasing can do this even
   * though `key` came from the conf's own iterator) or when it raises `IllegalStateException` (a
   * substitution cycle that never converges) -- either way, forwarding the raw literal preserves
   * the extraction's prior behavior for that entry rather than aborting the whole object store's
   * option extraction.
   */
  private def substitutedValue(
      hadoopConf: Configuration,
      key: String,
      rawValue: String): String = {
    try {
      Option(hadoopConf.get(key)).getOrElse(rawValue)
    } catch {
      case _: IllegalStateException => rawValue
    }
  }
}

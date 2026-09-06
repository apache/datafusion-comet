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
import org.apache.spark.sql.comet.util.Utils

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

  /**
   * The S3 settings Comet plumbs, in the three spellings it has to speak: the vendor-style
   * camelCase property (`fs.<alias>.<authority>.<vendorName>`), the Hadoop S3A suffix
   * (`fs.s3a.<s3aSuffix>`), and the iceberg-rust catalog key. The camelCase spellings are the
   * ones vendor-branded S3A forks expose; they are not a Hadoop or AWS standard. Single source of
   * truth so adding a setting is one edit rather than one per direction; the user-facing table in
   * `docs/source/user-guide/latest/datasources.md` mirrors the first two columns.
   */
  private val s3Properties: Seq[(String, String, String)] = Seq(
    ("awsAccessKeyId", "access.key", "s3.access-key-id"),
    ("awsSecretAccessKey", "secret.key", "s3.secret-access-key"),
    ("awsSessionToken", "session.token", "s3.session-token"),
    ("endpoint", "endpoint", "s3.endpoint"),
    ("region", "endpoint.region", "s3.region"),
    ("pathStyleAccess", "path.style.access", "s3.path-style-access"))

  // Vendor property -> `fs.s3a` suffix. Unknown properties are dropped.
  private val vendorPropertyToS3aSuffix: Map[String, String] =
    s3Properties.map { case (vendor, s3aSuffix, _) => vendor -> s3aSuffix }.toMap

  /** Maps a Hadoop `fs.s3a.*` property suffix to its GLOBAL iceberg-rust `s3.*` key. */
  private[comet] val s3aSuffixToIcebergGlobalKey: Map[String, String] =
    s3Properties.map { case (_, s3aSuffix, icebergKey) => s3aSuffix -> icebergKey }.toMap

  /** A URI's scheme, lowercased; None when it has none. */
  private[comet] def lowerScheme(uri: URI): Option[String] =
    Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT))

  // Comma-separated scheme list -> trimmed, lowercased set (case-insensitive). Shared with
  // CometScanRule's scheme gate so the JVM admit-decision and native rewrite parse identically.
  private[comet] def parseSchemeSet(raw: String): Set[String] =
    Option(raw)
      .map(Utils.stringToSeq(_).map(_.toLowerCase(Locale.ROOT)).toSet)
      .getOrElse(Set.empty)

  // Opt-in S3-compliant alias schemes from the Hadoop config `fs.comet.s3Compliant.schemes`
  // (comma-separated, trimmed, lowercased, empty/missing => none). The native gate no longer
  // claims them, so the opt-in is resolved wherever the Hadoop config is available: the scan gate
  // (CometScanRule) and the Iceberg write path's data-bucket resolution.
  private[comet] def resolveS3CompliantSchemes(hadoopConf: Configuration): Set[String] =
    parseSchemeSet(hadoopConf.get(COMET_S3_COMPLIANT_SCHEMES_KEY))

  /**
   * Java-callable view of [[resolveS3CompliantSchemes]], so `CometFileKeyUnwrapper` folds exactly
   * the schemes this object admits instead of parsing the config a second time. Public (not
   * `private[comet]`) so Scala emits the static forwarder the Java caller uses.
   */
  def resolveS3CompliantSchemesAsJava(hadoopConf: Configuration): java.util.Set[String] = {
    import scala.jdk.CollectionConverters._
    resolveS3CompliantSchemes(hadoopConf).asJava
  }

  /**
   * The S3 bucket a URI addresses: its authority, or the first path segment for the authorityless
   * `blob:///bucket/key` form (matching the native rewrite that promotes it into the host).
   * Returns None for a non-S3-family scheme, so a local Hadoop-catalog
   * `file:///tmp/warehouse/...` or a `gs://` location yields no bucket rather than a surprising
   * `tmp`: only `s3`/`s3a`/`s3n` and the opted-in `s3CompliantSchemes` share the `fs.s3a.*`
   * per-bucket surface. Callers therefore need no scheme check of their own.
   */
  private[comet] def bucketForUri(uri: URI, s3CompliantSchemes: Set[String]): Option[String] = {
    if (!lowerScheme(uri).exists(isS3FamilyScheme(_, s3CompliantSchemes))) {
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
   * Translate the vendor-style `fs.<scheme>.<authority>.<property>` keys collected by
   * `extractObjectStoreOptions` into the `fs.s3a.*` shape object_store's AmazonS3Builder reads.
   * Recognized properties are `vendorPropertyToS3aSuffix`; unknown ones are dropped.
   *
   * Keys land at the per-bucket `fs.s3a.bucket.<bucket>.*` scope (for the `default` authority the
   * bucket is `defaultBucket`, promoted from the URL path) -- the scope native `get_config`
   * checks first. The authority is taken greedily (split at the LAST dot) so dotted bucket names
   * survive.
   *
   * An `endpoint` also synthesizes `path.style.access=true` as a soft default, suppressed by an
   * explicit vendor `pathStyleAccess` or by a per-bucket `fs.s3a.*` setting the caller already
   * copied into `s3aOptions` (including `false`, the escape hatch). A GLOBAL
   * `fs.s3a.path.style.access` is deliberately not consulted: it may target other s3a workloads,
   * and per-bucket wins over global natively anyway.
   *
   * `vendorEntries` are `(authority.property, substituted value)` pairs, i.e. the key with its
   * `fs.<scheme>.` prefix already stripped.
   */
  private def translateVendorKeys(
      vendorEntries: Seq[(String, String)],
      s3aOptions: collection.Map[String, String],
      defaultBucket: Option[String]): Map[String, String] = {
    val out = scala.collection.mutable.Map[String, String]()

    val matched = vendorEntries.flatMap { case (authorityAndProperty, value) =>
      val dot = authorityAndProperty.lastIndexOf('.')
      if (dot <= 0) None
      else {
        vendorPropertyToS3aSuffix
          .get(authorityAndProperty.substring(dot + 1))
          .map(suffix => (authorityAndProperty.substring(0, dot), suffix, value))
      }
    }
    // Apply `default`-authority keys BEFORE explicit ones. A `default` key (promoted to the
    // URL-path bucket) and an explicit `fs.<scheme>.<bucket>.*` can resolve to the same
    // `fs.s3a.bucket.<bucket>.*` scope, and the explicit spelling must win -- which it does only
    // when written last. Otherwise the winner is whichever config entry is yielded last (hash
    // order), so the same config could pick either endpoint from one run to the next.
    val (defaults, explicit) = matched.partition(_._1 == vendorDefaultAuthority)

    (defaults ++ explicit).foreach { case (authority, suffix, value) =>
      val bucket =
        if (authority == vendorDefaultAuthority) defaultBucket else Some(authority)
      val scope = bucket.map(b => s"fs.s3a.bucket.$b").getOrElse("fs.s3a")
      out(s"$scope.$suffix") = value
      val userPinnedPathStyle =
        bucket.exists(b => s3aOptions.contains(s"fs.s3a.bucket.$b.path.style.access"))
      if (suffix == "endpoint" && !userPinnedPathStyle) {
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
    // default).
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

    // A configured S3-compliant alias also contributes vendor keys, but those must be applied
    // AFTER the whole fs.s3a.* pass (so real vendor values win a conflict). Collect them during
    // the single walk of the config below rather than making a second pass: `iterator()` rebuilds
    // a full copy of the property map on every call.
    // Empty when the scheme is not an opted-in alias, so the guard below costs one `nonEmpty`
    // rather than an Option allocation per Hadoop property.
    val vendorPrefix = if (s3CompliantSchemes.contains(scheme)) s"fs.$scheme." else ""
    val vendorEntries = scala.collection.mutable.ArrayBuffer[(String, String)]()

    hadoopConf.iterator().asScala.foreach { entry =>
      val key = entry.getKey
      if (prefixes.get.exists(prefix => key.startsWith(prefix))) {
        options(key) = substitutedValue(hadoopConf, key, entry.getValue)
      } else if (vendorPrefix.nonEmpty && key.startsWith(vendorPrefix)) {
        vendorEntries +=
          key.substring(vendorPrefix.length) -> substitutedValue(hadoopConf, key, entry.getValue)
      }
    }

    // Pass the resolved bucket so `fs.<scheme>.default.*` lands at that bucket's scope.
    if (vendorEntries.nonEmpty) {
      translateVendorKeys(vendorEntries.toSeq, options, bucketForUri(uri, s3CompliantSchemes))
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

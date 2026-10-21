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

import java.io.IOException
import java.net.{URI, URISyntaxException}
import java.util.Locale

import scala.collection.mutable.{Map => MutableMap}
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration

/**
 * Decides whether Comet's native S3 client would resolve the same S3 configuration as Hadoop's
 * `S3AFileSystem` for the buckets a scan touches. The native client builds its configuration from
 * the options [[NativeConfig.extractObjectStoreOptions]] forwards, and resolves them differently
 * from Hadoop in several ways (`lookupPassword` bucket precedence, JCEKS credential provider
 * aliases, the clear text fallback, assumed role session policies, provider class semantics,
 * scheme-less endpoints). A scan that went native over such a table would read under a different
 * effective identity or endpoint than Spark would, silently. Each gate here returns a decline
 * reason for the scan to fall back to Spark, or `None` when native and Hadoop agree.
 *
 * Shared by every native scan that forwards `fs.s3a.*` options (see
 * [[NativeConfig.extractScanObjectStoreOptions]]) and by the Delta contrib's own claim gates.
 * Correctness rule: when in doubt, decline.
 */
object S3ConfigGate {

  /**
   * First reason the native S3 client would not resolve the same configuration as Hadoop for any
   * bucket among `uris`, or `None` when it would, or when no URI is an `s3`/`s3a` one. The gates
   * are ordered cheapest and most specific first. They share one propagated-conf cache so each
   * bucket's full `Configuration` deep copy is made at most once.
   */
  def declineReason(hadoopConf: Configuration, uris: Seq[URI]): Option[String] = {
    if (!uris.exists(uri => s3Bucket(uri).isDefined)) {
      return None
    }
    val propagatedConfCache = MutableMap.empty[String, Configuration]
    unsupportedEncryptionAlgorithmReason(hadoopConf, uris, propagatedConfCache)
      .orElse(proxyGateReason(hadoopConf, uris, propagatedConfCache))
      .orElse(assumedRolePolicyGateReason(hadoopConf, uris, propagatedConfCache))
      .orElse(hadoopOnlyEndpointGateReason(hadoopConf, uris, propagatedConfCache))
      .orElse(s3ConfigDivergenceReason(hadoopConf, uris, propagatedConfCache))
      .orElse(providerClassGateReason(hadoopConf, uris))
  }

  /**
   * String-literal Hadoop conf keys consulted below. `hadoop-aws` is NOT on this module's runtime
   * classpath, so `org.apache.hadoop.fs.s3a.Constants` must never be referenced here (would raise
   * `NoClassDefFoundError` for sessions with no S3 dependency).
   */
  private val HadoopCredentialProviderPathKey = "hadoop.security.credential.provider.path"
  private val S3aCredentialProviderPathKey = "fs.s3a.security.credential.provider.path"

  /**
   * `CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_CLEAR_TEXT_FALLBACK`, default
   * `true`, verified via `javap` against `hadoop-common` 3.3.4's
   * `Configuration#getPasswordFromConfig`: `getPassword` only falls back to reading a plaintext
   * conf value once `getBoolean(<this key>, true)` holds -- with the flag off, a plaintext value
   * is invisible to every `getPassword`-based resolver, even when no credential provider is
   * configured at all.
   */
  private val ClearTextFallbackKey = "hadoop.security.credential.clear-text-fallback"

  private def s3aBucketProviderPathKey(bucket: String): String =
    s"fs.s3a.bucket.$bucket.security.credential.provider.path"

  /**
   * The LONG form of [[s3aBucketProviderPathKey]]: `S3AUtils#lookupPassword` resolves per-bucket
   * overrides through both a long key (`fs.s3a.bucket.B.<full base key>`) and a short key; both
   * must be covered here too.
   */
  private def s3aBucketLongProviderPathKey(bucket: String): String =
    s"fs.s3a.bucket.$bucket.fs.s3a.security.credential.provider.path"

  private def nonEmptyConf(hadoopConf: Configuration, key: String): Boolean =
    Option(hadoopConf.get(key)).exists(_.nonEmpty)

  /**
   * The lowercase-scheme-checked S3/S3A bucket name from `uri`'s authority, or `None` when
   * `uri`'s scheme is not `s3`/`s3a`. Parses the raw authority manually rather than
   * `URI#getHost`, avoiding the RFC 3986 `reg-name` pitfall (`getHost` returns `null` for the
   * whole authority).
   */
  private[comet] def s3Bucket(uri: URI): Option[String] = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT))
    if (scheme.contains("s3") || scheme.contains("s3a")) {
      val authority = Option(uri.getAuthority).getOrElse("")
      val at = authority.lastIndexOf('@')
      val hostAndPort = if (at >= 0) authority.substring(at + 1) else authority
      val colon = hostAndPort.lastIndexOf(':')
      val host = if (colon >= 0) hostAndPort.substring(0, colon) else hostAndPort
      if (host.isEmpty) None else Some(host)
    } else {
      None
    }
  }

  private def plainValue(hadoopConf: Configuration, key: String): Option[String] =
    Option(hadoopConf.get(key)).filter(_.nonEmpty)

  /**
   * How Hadoop's OWN consumer reads one of the keys compared by [[s3ConfigDivergenceReason]],
   * which decides how [[s3KeyDivergenceReason]] computes the Hadoop-effective side of its
   * equality check. Exactly two consumer families exist among [[AllS3ConfigKeys]] in `hadoop-aws`
   * 3.3.4, each verified via `javap`/CFR against the real call sites (cited per key on
   * [[S3ConfigKeyConsumers]]). The tier must mirror the key's ACTUAL consumer: resolving a
   * [[PropagatedOptionConsumer]] key through the wider `lookupPassword` cascade is NOT fail-safe
   * for a value-EQUALITY comparator -- a long-form alias value Hadoop itself never reads can
   * EQUAL native's resolution while Hadoop's true propagate-then-plain-get value differs, turning
   * a real divergence into a wrongly-claimed scan (the endpoint `${...}`-redirect shape pinned in
   * `S3ConfigGateSuite`).
   */
  private[comet] sealed trait S3ConfigConsumer

  /**
   * Read via `S3AUtils#lookupPassword(bucket, conf, baseKey)`, verified via `javap` against
   * `hadoop-aws` 3.3.4: builds `longBucketKey = "fs.s3a.bucket." + bucket + "." + baseKey` (the
   * FULL, already-`fs.s3a`-prefixed base key appended after the bucket segment) and reads it via
   * `Configuration#getPassword` BEFORE the short-bucket key, keeping the long value whenever
   * `getPassword` returns non-empty and only falling through to short-then-global otherwise.
   * `getPassword` is Hadoop-credential-provider-aware and skips plaintext conf entirely when
   * [[ClearTextFallbackKey]] is false. Modeled by [[hadoopLookupPasswordEffective]].
   */
  private[comet] case object LookupPasswordConsumer extends S3ConfigConsumer

  /**
   * Read via `S3AUtils#propagateBucketOptions` followed by a plain `Configuration#get`-family
   * call (`getTrimmed`/`getBoolean`/`getClasses`) against the propagated view: the short bucket
   * form wins only by having overwritten the global key during propagation, the long bucket form
   * folds into an unread `fs.s3a.fs.s3a.*` key, and neither a credential provider nor
   * [[ClearTextFallbackKey]] is ever consulted. Modeled as a plain `Configuration#get` on the
   * [[propagateBucketOptions]] result, which also expands `${...}` references under that
   * propagated view exactly like the real consumer.
   */
  private[comet] case object PropagatedOptionConsumer extends S3ConfigConsumer

  /**
   * Every `fs.s3a.*` base key that governs whether a claimed native scan actually behaves like
   * Hadoop's own reader would, paired with the consumer family Hadoop resolves it through -- ONE
   * list, with each key's resolution tier declared beside it, so a key can never sit in the
   * comparator without a deliberate classification (adding one without picking a tier does not
   * compile). The entries are every per-bucket `fs.s3a.*` base key native's S3 client's
   * `get_config` (s3.rs) resolves, verified directly against its call sites:
   * `extract_s3_config_options` (endpoint.region, path.style.access, endpoint,
   * requester.pays.enabled), `lookup_provider_class` (the Comet-specific
   * credential-provider-class activation key), and
   * `build_credential_provider`/`build_aws_credential_provider_metadata`/
   * `build_assume_role_credential_provider_metadata` (aws.credentials.provider,
   * assumed.role.credentials.provider, assumed.role.arn, assumed.role.session.name).
   *
   * Tier assignments, each verified via `javap`/CFR against `hadoop-aws` 3.3.4:
   *   - access.key/secret.key/session.token: `S3AUtils#getAWSAccessKeys` and
   *     `MarshalledCredentialBinding#fromFileSystem` (reached from
   *     `TemporaryAWSCredentialsProvider`) resolve all three via `S3AUtils#lookupPassword` --
   *     [[LookupPasswordConsumer]].
   *   - aws.credentials.provider and assumed.role.credentials.provider:
   *     `S3AUtils#buildAWSProviderList` -> `loadAWSProviderClasses` -> plain
   *     `Configuration#getClasses` -- [[PropagatedOptionConsumer]].
   *   - assumed.role.arn/session.name: `AssumedRoleCredentialProvider`'s constructor reads both
   *     via plain `Configuration#getTrimmed` -- [[PropagatedOptionConsumer]].
   *   - endpoint (`S3AFileSystem`: `getTrimmed`), endpoint.region (`DefaultS3ClientFactory`:
   *     `getTrimmed`), path.style.access (`S3AFileSystem`: `getBoolean`) --
   *     [[PropagatedOptionConsumer]].
   *   - requester.pays.enabled: not read anywhere in `hadoop-aws` 3.3.4 (the constant does not
   *     even exist in its `Constants` class); later releases read it via plain `getBoolean`
   *     against the propagated conf, so the plain tier is both the faithful forward model and
   *     inert on 3.3.4 -- [[PropagatedOptionConsumer]].
   *   - comet.credential.provider.class: Comet's own activation key, plain conf read on both
   *     sides, never a Hadoop key at all -- [[PropagatedOptionConsumer]].
   *
   * SYNC NOTE: the key list must stay a superset of native's `NATIVE_S3A_CONFIG_PROPERTIES`
   * constant (`native/core/src/parquet/objectstore/s3.rs`, property suffixes without the
   * `fs.s3a.` prefix) -- `S3ConfigGateSuite`'s discovery-harness test asserts this mechanically
   * against [[AllS3ConfigKeys]]. A Rust test in `s3.rs` also parses this `Seq(` block as text, so
   * each entry must stay a single `"fs.s3a.<key>" -> <Tier>` literal pair. Literal strings, not
   * the [[AwsCredentialsProviderKey]] / [[AssumedRoleCredentialsProviderKey]] vals declared
   * below, purely to avoid a forward reference inside this `object` body; kept textually
   * identical to those two constants.
   */
  private[comet] val S3ConfigKeyConsumers: Seq[(String, S3ConfigConsumer)] = Seq(
    "fs.s3a.access.key" -> LookupPasswordConsumer,
    "fs.s3a.secret.key" -> LookupPasswordConsumer,
    "fs.s3a.session.token" -> LookupPasswordConsumer,
    "fs.s3a.aws.credentials.provider" -> PropagatedOptionConsumer,
    "fs.s3a.assumed.role.arn" -> PropagatedOptionConsumer,
    "fs.s3a.assumed.role.session.name" -> PropagatedOptionConsumer,
    "fs.s3a.assumed.role.credentials.provider" -> PropagatedOptionConsumer,
    "fs.s3a.endpoint" -> PropagatedOptionConsumer,
    "fs.s3a.endpoint.region" -> PropagatedOptionConsumer,
    "fs.s3a.path.style.access" -> PropagatedOptionConsumer,
    "fs.s3a.requester.pays.enabled" -> PropagatedOptionConsumer,
    "fs.s3a.comet.credential.provider.class" -> PropagatedOptionConsumer)

  /** The compared keys alone, in [[S3ConfigKeyConsumers]] order (discovery-harness surface). */
  private[comet] val AllS3ConfigKeys: Seq[String] = S3ConfigKeyConsumers.map(_._1)

  /**
   * The short-bucket-then-global value resolved for `baseKey` under `bucket` from `hadoopConf`,
   * skipping an empty value at either alias exactly like [[plainValue]]. NOT used by
   * [[s3ConfigDivergenceReason]]/[[s3KeyDivergenceReason]] any more -- every key checked there
   * resolves per its declared [[S3ConfigKeyConsumers]] tier (see [[s3KeyDivergenceReason]]),
   * neither of which matches this function's read. This function's one remaining caller is
   * [[shortThenGlobalOrReason]], which reads provider-CLASS strings (from the ORIGINAL,
   * unpropagated conf) for name-support validation in [[providerClassReason]]/
   * [[assumedRoleProviderClassReason]] -- by the time those run, [[s3ConfigDivergenceReason]] has
   * already proven Hadoop's and native's effective values agree for the same key, so whichever of
   * the two (equal) values this narrower read returns does not affect correctness there. NEVER
   * used to compute native's own effective value -- see [[nativeShortThenGlobal]] for that.
   */
  private def shortThenGlobal(
      hadoopConf: Configuration,
      bucket: String,
      baseKey: String): Option[String] = {
    val shortKey = s"fs.s3a.bucket.$bucket." + baseKey.stripPrefix("fs.s3a.")
    plainValue(hadoopConf, shortKey).orElse(plainValue(hadoopConf, baseKey))
  }

  /**
   * The short-bucket-then-global value native's `get_config` (s3.rs) resolves for `baseKey` under
   * `bucket` from the ORIGINAL, unpropagated `hadoopConf` -- `NativeConfig
   * .extractObjectStoreOptions` forwards `Configuration#get`'s substituted value for every
   * `fs.s3a.*` entry with no bucket-option propagation step of its own, so the original conf is
   * the right input here. Unlike [[shortThenGlobal]]/[[plainValue]], this mirrors `get_config`
   * faithfully: PRESENCE of the short-bucket key alone -- never its emptiness -- decides whether
   * native falls back to the global key (`get_config` is a plain `HashMap::get`, which returns
   * `Some` for a key explicitly set to `""`), so an explicitly empty or whitespace-only
   * short-bucket value resolves to `Some("")` here and never falls through to global -- the
   * OPPOSITE of Hadoop's own `getPassword`/`lookupPassword` semantics (see
   * [[hadoopLookupPasswordEffective]]), which treat empty as absent and keep trying the next
   * alias. The ONLY function used to compute native's effective value in
   * [[s3KeyDivergenceReason]].
   *
   * Deliberately does NOT apply `get_config_trimmed`'s `.trim()` here: [[s3KeyDivergenceReason]]
   * trims both this value and Hadoop's effective value together, symmetrically, at the point they
   * are compared, rather than one-sidedly here -- trimming only the native side would flag a
   * spurious divergence for a value neither side's whitespace actually changes the behavior of
   * once each side's own downstream parsing normalizes it (e.g. Hadoop's own multi-line
   * `fs.s3a.aws.credentials.provider` default, which both Hadoop and native additionally trim per
   * comma-separated entry after splitting), while a one-sided trim would make an
   * otherwise-identical default value look diverged for every bucket, never claiming natively at
   * all.
   */
  private def nativeShortThenGlobal(
      hadoopConf: Configuration,
      bucket: String,
      baseKey: String): Option[String] = {
    val shortKey = s"fs.s3a.bucket.$bucket." + baseKey.stripPrefix("fs.s3a.")
    Option(hadoopConf.get(shortKey)).orElse(Option(hadoopConf.get(baseKey)))
  }

  /**
   * Faithful in-memory replica of `S3AUtils#propagateBucketOptions` (`hadoop-aws`), which
   * `S3AFileSystem#initialize` calls FIRST, before any option or credential is read:
   * `Configuration conf = propagateBucketOptions(originalConf, bucket); ...; setConf(conf);` --
   * every subsequent `conf.get`/`getPassword` call in that filesystem instance, including
   * `${...}` variable substitution, resolves against this propagated view, not the original conf.
   * `hadoop-aws` is not on this module's runtime classpath (see the string-literal-keys note
   * above), so `S3AUtils#propagateBucketOptions` cannot be called directly; this reproduces its
   * logic verbatim using only `hadoop-common`'s `Configuration`:
   * {{{
   * public static Configuration propagateBucketOptions(Configuration source, String bucket) {
   *   final String bucketPrefix = FS_S3A_BUCKET_PREFIX + bucket + '.';
   *   final Configuration dest = new Configuration(source);
   *   for (Map.Entry<String, String> entry : source) {
   *     final String key = entry.getKey();
   *     final String value = entry.getValue(); // the (unexpanded) value
   *     if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) continue;
   *     final String stripped = key.substring(bucketPrefix.length());
   *     if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
   *       // ignored
   *     } else {
   *       final String generic = FS_S3A_PREFIX + stripped;
   *       dest.set(generic, value, ...); // overwrites any existing global value
   *     }
   *   }
   *   return dest;
   * }
   * }}}
   * Note the LONG bucket form (`fs.s3a.bucket.B.fs.s3a.<key>`) folds to an unread
   * `fs.s3a.fs.s3a.<key>` key here too, exactly like the real method -- `stripped` already starts
   * with `fs.s3a.` in that case, so prepending `fs.s3a.` again produces a key nothing ever reads.
   */
  private def propagateBucketOptions(hadoopConf: Configuration, bucket: String): Configuration = {
    val bucketPrefix = s"fs.s3a.bucket.$bucket."
    val dest = new Configuration(hadoopConf)
    hadoopConf.iterator().asScala.foreach { entry =>
      val key = entry.getKey
      if (key.startsWith(bucketPrefix) && key != bucketPrefix) {
        val stripped = key.substring(bucketPrefix.length)
        if (!stripped.startsWith("bucket.") && stripped != "impl") {
          dest.set(s"fs.s3a.$stripped", entry.getValue)
        }
      }
    }
    dest
  }

  /**
   * Canonical and deprecated Hadoop S3A encryption-algorithm config keys, verified via `javap`
   * against `hadoop-aws` 3.3.4's `org.apache.hadoop.fs.s3a.Constants`: `S3_ENCRYPTION_ALGORITHM =
   * "fs.s3a.encryption.algorithm"` (canonical) and `SERVER_SIDE_ENCRYPTION_ALGORITHM =
   * "fs.s3a.server-side-encryption-algorithm"` (DEPRECATED -- note the hyphen before "algorithm",
   * unlike the corresponding `*.key` constants below, which both use a `.key` suffix).
   * `hadoop-aws` is NOT on this module's runtime classpath, so these stay string literals, same
   * rationale as [[HadoopCredentialProviderPathKey]].
   */
  private val S3EncryptionAlgorithmKey = "fs.s3a.encryption.algorithm"
  private val DeprecatedS3EncryptionAlgorithmKey = "fs.s3a.server-side-encryption-algorithm"

  /**
   * The exact strings `S3AEncryptionMethods#getMethod` accepts, verified via `javap`/CFR against
   * `hadoop-aws` 3.3.4's `S3AEncryptionMethods` enum: `NONE("")`, `SSE_S3("AES256", serverSide =
   * true, requiresSecret = false)`, `SSE_KMS("SSE-KMS", serverSide = true, requiresSecret =
   * false)`, `SSE_C("SSE-C", serverSide = true, requiresSecret = true)`, `CSE_KMS("CSE-KMS",
   * serverSide = false, requiresSecret = true)`, `CSE_CUSTOM("CSE-CUSTOM", serverSide = false,
   * requiresSecret = true)`. `getMethod` parses case-insensitively
   * (`values().find(_.getMethod.equalsIgnoreCase(algorithm))`), matched below the same way.
   *
   * ALLOWLIST, not a blocklist (replaces the former SSE-C-only blocklist): only the algorithms S3
   * decrypts transparently on GET/HEAD given read permission alone, with NO extra request header
   * and NO client-side step, are safe for a native scan that forwards none of Hadoop's
   * `fs.s3a.encryption.*`/`fs.s3a.server-side-encryption*` options --
   *   - `AES256` (SSE_S3, `serverSide = true`): plain server-side encryption, transparent on GET.
   *   - `SSE-KMS` (SSE_KMS, `serverSide = true`): server-side, KMS-managed key, transparent on
   *     GET given KMS decrypt permission (no header).
   *   - `DSSE-KMS`: NOT present in this enum on `hadoop-aws` 3.3.4 (confirmed by the six values
   *     listed above) -- `S3AEncryptionMethods.getMethod("DSSE-KMS")` throws
   *     `IOException("Unknown encryption algorithm DSSE-KMS")` on this version, so
   *     `S3AUtils#buildEncryptionSecrets` (and therefore Hadoop's own reader) already fails
   *     before ever reading such a table under 3.3.4, meaning this string can never actually be
   *     the resolved value on the declared target version -- admitting it here is inert there.
   *     Included anyway, forward-compatible, for a newer `hadoop-aws` on the runtime classpath (a
   *     later Hadoop release; this module has no compile-time `hadoop-aws` dependency, see the
   *     string-literal-keys note above) where DSSE-KMS is a real, dual-layer, server-side
   *     algorithm decrypted transparently on GET the same way SSE-KMS is. Every other value
   *     declines: `SSE-C` (SSE_C is `serverSide = true` in Hadoop's own enum, but `requiresSecret
   *     \= true` -- S3 rejects a GET/HEAD for an SSE-C object outright (400 Bad Request) unless
   *     the customer key is resent as a request header on every call, so a native scan that never
   *     learns the key cannot succeed at all, where Hadoop's own reader -- whose request factory
   *     attaches the key -- would), `CSE-KMS`/`CSE-CUSTOM` (`serverSide = false`: client-side
   *     encryption decrypts object bytes locally in the SDK layer, which the native Parquet
   *     reader has no equivalent of -- it would read raw ciphertext), and any future/unknown
   *     value (a value `S3AEncryptionMethods.getMethod` itself would reject is certainly not one
   *     of the three confirmed-transparent algorithms above; declining is the only safe default
   *     for anything this gate cannot positively confirm).
   */
  private val AllowedEncryptionAlgorithms: Set[String] = Set("AES256", "SSE-KMS", "DSSE-KMS")

  /**
   * `bucket`'s effective encryption-algorithm key and value under `hadoopConf`, or `None` when
   * neither the canonical nor deprecated key is set anywhere consulted. Mirrors
   * `S3AUtils#buildEncryptionSecrets`'s real resolution order, verified via `javap`/CFR
   * decompilation of `hadoop-aws` 3.3.4's `S3AUtils.class`:
   * {{{
   * String algorithm = lookupBucketSecret(bucket, conf, "fs.s3a.encryption.algorithm");
   * if (algorithm == null)
   *   algorithm = lookupBucketSecret(bucket, conf, "fs.s3a.server-side-encryption-algorithm");
   * if (algorithm == null)
   *   algorithm = lookupPassword(null, conf, "fs.s3a.encryption.algorithm");
   * if (algorithm == null)
   *   algorithm = lookupPassword(null, conf, "fs.s3a.server-side-encryption-algorithm");
   * }}}
   * i.e. bucket-tier (canonical, then deprecated), THEN global-tier (canonical, then deprecated)
   * -- the two tiers are never interleaved key-by-key, so this must stay two explicit bucket-tier
   * lookups followed by two explicit global-tier lookups, not a single
   * [[hadoopLookupPasswordEffective]] call per key (which would let an unset canonical bucket key
   * fall through straight to the canonical GLOBAL value ahead of a SET deprecated bucket key, the
   * wrong answer).
   *
   * The three-argument `lookupPassword` defaults to `""`, not `null`, so the last call in the
   * block above never runs: Hadoop reads the deprecated global key only through `Configuration`'s
   * deprecation alias of the canonical one. This function still reads it as a fourth lookup,
   * which can only add a decline.
   *
   * THE FIX for the SSE-C long-bucket-alias gap is entirely inside the bucket tier:
   * `lookupBucketSecret` itself is long-then-short, decompiled from `hadoop-aws` 3.3.4's
   * `S3AUtils.class`:
   * {{{
   * // longBucketKey  = fs.s3a.bucket.B.fs.s3a.<key>
   * String longBucketKey = String.format(BUCKET_PATTERN, bucket, baseKey);
   * String initialVal = getPassword(conf, longBucketKey, null, null);
   * // shortBucketKey = fs.s3a.bucket.B.<key>
   * String shortBucketKey = String.format(BUCKET_PATTERN, bucket, subkey);
   * // keeps initialVal (the LONG value) if non-empty
   * return getPassword(conf, shortBucketKey, initialVal, null);
   * }}}
   * i.e. the SAME long-bucket-key construction and long-wins-if-nonempty semantics as
   * `S3AUtils#lookupPassword` (see [[LookupPasswordConsumer]]/[[hadoopLookupPasswordEffective]]).
   * `buildEncryptionSecrets` runs on the [[propagateBucketOptions]] view, so
   * [[unsupportedEncryptionAlgorithmReason]] calls this on that view (and on the raw conf, which
   * can only add a decline). An earlier version of this function modeled the bucket tier as
   * SHORT-only, documented as "the LONG bucket form is genuinely never consulted for this key" --
   * that documentation was wrong (this decompilation supersedes it): a bucket configured only via
   * `fs.s3a.bucket.B.fs.s3a.encryption.algorithm=SSE-C` bypassed the SSE-C gate entirely, because
   * Hadoop's own reader DOES read that long form (and picks SSE-C), while this function reported
   * `None` (nothing set) and the allowlist check below never even ran.
   *
   * The canonical-vs-deprecated distinction below is frequently moot in practice: `hadoop-aws`'s
   * `S3AFileSystem.addDeprecatedKeys()` statically registers `fs.s3a.server-side-encryption-*` as
   * `Configuration`-level deprecated aliases of `fs.s3a.encryption.*` (verified via `javap`), a
   * registration that lives in a static field on Hadoop's `Configuration` class -- process-wide
   * once `S3AFileSystem`'s class has loaded anywhere in the JVM, which a real scan has always
   * already done by the time this gate runs, since reading the S3 table at all requires loading
   * that class. Once active, `Configuration#get` resolves either literal key to the identical
   * value transparently, making the two-key cascade below redundant (but harmless) for that case;
   * it remains the operative path only when nothing else in the process has loaded
   * `S3AFileSystem` yet.
   *
   * ALSO walks the Hadoop-credential-provider (JCEKS) path via [[resolveViaCredentialAliases]]
   * for each of the four lookups below, matching `lookupBucketSecret`/`lookupPassword`'s real
   * per-alias `getPassword` calls (quoted above) exactly: both are `getPassword`, not plain
   * `Configuration#get`, so a bucket storing the algorithm name ONLY in a JCEKS keystore is
   * exactly as real a Hadoop deployment shape for this key as it is for the credential keys
   * [[hadoopLookupPasswordEffective]] already covers -- there is nothing algorithm-specific that
   * makes JCEKS storage implausible here, so an earlier version of this function skipping it
   * (documented at the time as "the algorithm NAME is not credential-sensitive data, so storing
   * it in a keystore is not a realistic Hadoop deployment pattern") was an unjustified, narrower
   * read than Hadoop's own resolver actually performs, under-declining a bucket whose algorithm
   * is keystore-only. [[resolveViaCredentialAliases]]'s Arm B/C split still means this is zero
   * extra I/O for the common case: keystore I/O only happens when a Hadoop credential-provider
   * path is actually configured for the bucket, contained in that function's own try/catch.
   * `bucketTier`/`globalTier` return `Left` (propagated straight through by [[orElseTier]]) when
   * [[resolveViaCredentialAliases]] cannot safely verify a tier at all (an S3A-scoped provider
   * path, or a corrupt/unreadable global keystore) -- correctly short-circuiting the whole
   * cascade with a decline rather than silently falling through to a later tier that might look
   * unset only because the true value was unverifiable.
   */
  private def effectiveEncryptionAlgorithm(
      hadoopConf: Configuration,
      bucket: String): Either[String, Option[(String, String)]] = {
    def bucketTier(baseKey: String): Either[String, Option[(String, String)]] = {
      val longKey = s"fs.s3a.bucket.$bucket.$baseKey"
      val shortKey = s"fs.s3a.bucket.$bucket." + baseKey.stripPrefix("fs.s3a.")
      resolveViaCredentialAliases(hadoopConf, bucket, Seq(longKey, shortKey))
        .map(_.map(baseKey -> _))
    }
    def globalTier(baseKey: String): Either[String, Option[(String, String)]] =
      resolveViaCredentialAliases(hadoopConf, bucket, Seq(baseKey))
        .map(_.map(baseKey -> _))

    // Short-circuits on Left (unverifiable tier) or Right(Some(_)) (resolved); only Right(None)
    // (tier definitively unset) falls through to `next`, mirroring buildEncryptionSecrets's
    // sequential `if (algorithm == null) algorithm = ...` cascade exactly.
    def orElseTier(
        current: Either[String, Option[(String, String)]],
        next: => Either[String, Option[(String, String)]])
        : Either[String, Option[(String, String)]] =
      current match {
        case Left(reason) => Left(reason)
        case Right(Some(value)) => Right(Some(value))
        case Right(None) => next
      }

    orElseTier(
      bucketTier(S3EncryptionAlgorithmKey),
      orElseTier(
        bucketTier(DeprecatedS3EncryptionAlgorithmKey),
        orElseTier(
          globalTier(S3EncryptionAlgorithmKey),
          // Hadoop reaches this key only through the deprecation alias (see above); reading
          // it directly can only over-decline.
          globalTier(DeprecatedS3EncryptionAlgorithmKey))))
  }

  /**
   * The only algorithm values a decline reason may print. Any other resolved value is withheld,
   * since a `${...}` reference can expand the algorithm key to a customer key or token.
   */
  private val PrintableEncryptionAlgorithms: Seq[String] =
    Seq("AES256", "SSE-S3", "SSE-KMS", "DSSE-KMS", "SSE-C", "CSE-KMS", "CSE-CUSTOM")

  private def unsupportedEncryptionAlgorithmDeclineReason(
      bucket: String,
      algorithmKey: String,
      algorithm: String): String = {
    val setting = PrintableEncryptionAlgorithms
      .find(_.equalsIgnoreCase(algorithm))
      .map(name => s"$algorithmKey=$name")
      .getOrElse(s"$algorithmKey set to a value that is not a recognised S3 encryption algorithm")
    s"Native scan does not support $setting for $bucket " +
      "(the native S3 client only supports unencrypted objects and S3's transparent " +
      "server-side algorithms -- AES256/SSE-S3, SSE-KMS, and DSSE-KMS decrypt on GET/HEAD given " +
      "read permission alone, with no extra request header; SSE-C additionally requires the " +
      "customer-provided key resent as a header on every GET/HEAD request, which the native S3 " +
      "client's extract_s3_config_options never forwards, and CSE-KMS/CSE-CUSTOM decrypt object " +
      "bytes client-side, a layer the native Parquet reader does not have -- any of these would " +
      "fail outright or silently read ciphertext where Hadoop's own reader succeeds)"
  }

  /**
   * First reason any bucket among `uris` is configured for an encryption algorithm the native S3
   * client cannot safely read, or `None` when claimable. Allowlist-based (see
   * [[AllowedEncryptionAlgorithms]]): only `AES256`/`SSE-KMS`/`DSSE-KMS` (and unset/empty) pass;
   * every other resolved value -- `SSE-C`, `CSE-KMS`, `CSE-CUSTOM`, or any unrecognized future
   * algorithm string -- declines. Deliberately NOT a blocklist keyed on `SSE-C` alone: an
   * allowlist is safe by construction against a Hadoop release adding a new encryption method
   * this gate has never heard of, where a blocklist would silently admit it. Never interpolates a
   * resolved key value, only key names, the bucket, and the (non-secret) algorithm name. Declines
   * on a `Left` from [[effectiveEncryptionAlgorithm]] too (an unverifiable credential-provider
   * arm, e.g. an S3A-scoped provider path or a corrupt/unreadable global keystore) -- the
   * algorithm cannot be ruled safe when it cannot be read at all.
   */
  private[comet] def unsupportedEncryptionAlgorithmReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        try {
          // Hadoop resolves the algorithm on the propagated view, where a bucket override can
          // change what a `${...}` reference expands to. The raw view is checked as well, so a
          // disagreement between the two can only decline.
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          encryptionAlgorithmReason(propagatedConf, bucket)
            .orElse(encryptionAlgorithmReason(hadoopConf, bucket))
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, S3EncryptionAlgorithmKey, e))
        }
      }
    }
  }

  private def encryptionAlgorithmReason(conf: Configuration, bucket: String): Option[String] =
    effectiveEncryptionAlgorithm(conf, bucket) match {
      case Left(reason) => Some(reason)
      case Right(None) => None
      case Right(Some((key, value))) =>
        if (!AllowedEncryptionAlgorithms.exists(_.equalsIgnoreCase(value))) {
          Some(unsupportedEncryptionAlgorithmDeclineReason(bucket, key, value))
        } else {
          None
        }
    }

  /**
   * Canonical Hadoop S3A HTTP-proxy host config key, verified via CFR decompilation of
   * `hadoop-aws` 3.3.4's `S3AUtils.class` (`initProxySupport`):
   * {{{
   * String proxyHost = conf.getTrimmed("fs.s3a.proxy.host", "");
   * int proxyPort = conf.getInt("fs.s3a.proxy.port", -1);
   * if (!proxyHost.isEmpty()) {
   *   ...
   *   String proxyUsername =
   *       S3AUtils.lookupPassword(bucket, conf, "fs.s3a.proxy.username", null, null);
   *   String proxyPassword =
   *       S3AUtils.lookupPassword(bucket, conf, "fs.s3a.proxy.password", null, null);
   *   ...
   * }
   * }}}
   * `fs.s3a.proxy.host`/`fs.s3a.proxy.port` resolve via a PLAIN, non-bucket-scoped, non-JCEKS
   * `Configuration#getTrimmed`/`getInt` call -- NOT `lookupPassword` -- against whatever conf
   * `S3AFileSystem#initialize` already ran through `propagateBucketOptions` before
   * `createAwsConf`/`initProxySupport` ever runs; only the SIBLING `fs.s3a.proxy.username`/
   * `fs.s3a.proxy.password` keys go through `lookupPassword` (bucket long/short/global,
   * JCEKS-aware). So the host is bucket-aware only via `propagateBucketOptions`'s short-bucket-
   * form folding, never the long-bucket form, and never a credential-provider read -- the SAME
   * shape as `endpoint`/`path.style.access` ([[PropagatedOptionConsumer]], see
   * [[S3ConfigKeyConsumers]]'s doc), not the credential family. `hadoop-aws` 3.4.x (the Spark 4.x
   * profiles' version) moves this code to `AWSClientConfig#createProxyConfiguration`/
   * `#createAsyncProxyConfiguration` but keeps the exact same reads, verified via `javap` against
   * 3.4.2: `conf.getTrimmed("fs.s3a.proxy.host", "")` for the host, `S3AUtils.lookupPassword` for
   * username/password only.
   *
   * [[proxyGateReason]] therefore resolves the host EXACTLY like its real consumer -- plain
   * `Configuration#getTrimmed` on the [[propagateBucketOptions]] result, no provider arms, no
   * [[ClearTextFallbackKey]] handling -- rather than through the wider `lookupPassword` cascade
   * an earlier version reused here. The wider cascade was wrong in BOTH directions for this key:
   * with only the GLOBAL Hadoop provider path set and [[ClearTextFallbackKey]] false,
   * `getPassword` hides a plaintext host that `getTrimmed` serves to Hadoop anyway (a missed
   * decline, the exact bypass this gate exists to close -- native has NO HTTP-proxy support of
   * any kind, no `fs.s3a.proxy.*` key is read anywhere in `s3.rs`); and an S3A-scoped provider
   * path or a lone long-form bucket alias declined a bucket whose real consumer can never see a
   * host from either source (pure over-refusal -- no keystore can supply the host to a plain
   * `getTrimmed`, and the long form folds into the unread `fs.s3a.fs.s3a.proxy.host`).
   */
  private val S3ProxyHostKey = "fs.s3a.proxy.host"

  private def unsupportedProxyReason(bucket: String, key: String): String =
    s"Native scan does not support $key configured for $bucket (the native S3 client has " +
      "no HTTP proxy support at all -- no fs.s3a.proxy.* key is read anywhere in its object " +
      "store layer -- so a claimed scan would connect to S3 directly instead of routing through " +
      "the configured proxy, either bypassing an egress/network-segmentation policy or simply " +
      "failing to reach the endpoint)"

  /**
   * First reason any bucket among `uris` has an HTTP proxy configured via [[S3ProxyHostKey]], or
   * `None` when claimable. Reads the host EXACTLY like its real consumer (see
   * [[S3ProxyHostKey]]'s doc): plain `Configuration#getTrimmed` on the [[propagateBucketOptions]]
   * result, so this gate is always zero-I/O -- no credential provider is ever consulted for the
   * host, because none ever supplies it to Hadoop either. Ordered alongside
   * [[unsupportedEncryptionAlgorithmReason]] among the conf-only gates, ahead of
   * [[s3ConfigDivergenceReason]]. The try/catch guards `Configuration#get`'s
   * `IllegalStateException` on a `${...}` substitution cycle, same as [[s3KeyDivergenceReason]].
   * Never interpolates a resolved value: the proxy HOST is not secret, but naming it here would
   * be a strange place to first surface it, and proxy CREDENTIALS
   * (`fs.s3a.proxy.username`/`fs.s3a.proxy.password`, not read by this gate at all -- the whole
   * point of gating on the host is that a non-empty host declines before any proxy credential
   * would ever need to be forwarded) must never appear in a decline reason regardless.
   */
  private[comet] def proxyGateReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        try {
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          if (propagatedConf.getTrimmed(S3ProxyHostKey, "").nonEmpty) {
            Some(unsupportedProxyReason(bucket, S3ProxyHostKey))
          } else {
            None
          }
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, S3ProxyHostKey, e))
        }
      }
    }
  }

  /**
   * Canonical Hadoop S3A assumed-role session-policy key. `AssumedRoleCredentialProvider`'s
   * constructor reads it via a plain `Configuration#getTrimmed` against the
   * [[propagateBucketOptions]]-propagated conf (verified via `javap` against `hadoop-aws` 3.3.4
   * and 3.4.1: `conf.getTrimmed("fs.s3a.assumed.role.policy", "")` -- the same consumer shape as
   * `assumed.role.arn`/`session.name`, see [[S3ConfigKeyConsumers]]) and, when non-empty,
   * attaches it as the session policy of its STS AssumeRole request.
   */
  private val S3AssumedRolePolicyKey = "fs.s3a.assumed.role.policy"

  private def assumedRolePolicyReason(bucket: String, key: String): String =
    s"Native scan does not support $key configured for $bucket (Hadoop sends the " +
      "configured session policy in its STS AssumeRole request, but the native S3 client's " +
      "assumed-role provider never reads or forwards this key, so a claimed scan would " +
      "assume the role WITHOUT the configured session restriction -- silently widening the " +
      "effective permissions instead of failing)"

  /**
   * First reason any bucket among `uris` configures an assumed-role session policy via
   * [[S3AssumedRolePolicyKey]], or `None` when claimable. Hadoop's
   * `AssumedRoleCredentialProvider` includes the policy in its AssumeRole request; native's
   * assumed-role provider does not, so a configured policy must decline until native supports it.
   * Resolved exactly like the key's real consumer (plain `getTrimmed` on the propagated conf,
   * mirroring [[proxyGateReason]]); declined whenever set, whether or not the current provider
   * chain names the assumed-role provider -- a policy that is dead config today can become live
   * through a provider-chain change native never re-validates. Never interpolates the policy
   * document itself, only the key and bucket.
   */
  private[comet] def assumedRolePolicyGateReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined || cometProviderClassConfigured(hadoopConf, bucket)) {
        declined
      } else {
        try {
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          if (propagatedConf.getTrimmed(S3AssumedRolePolicyKey, "").nonEmpty) {
            Some(assumedRolePolicyReason(bucket, S3AssumedRolePolicyKey))
          } else {
            None
          }
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, S3AssumedRolePolicyKey, e))
        }
      }
    }
  }

  /** Hadoop's S3A SSL switch; `S3AFileSystem` reads it via `Configuration#getBoolean`. */
  private val S3SslEnabledKey = "fs.s3a.connection.ssl.enabled"
  private val S3EndpointKey = "fs.s3a.endpoint"

  /**
   * Hadoop's assumed-role STS endpoint keys; `AssumedRoleCredentialProvider` sends AssumeRole to
   * the configured endpoint, while native builds its provider with SDK defaults.
   */
  private val S3StsEndpointKeys =
    Seq("fs.s3a.assumed.role.sts.endpoint", "fs.s3a.assumed.role.sts.endpoint.region")

  private def insecureEndpointReason(bucket: String): String =
    s"Native scan does not support a scheme-less $S3EndpointKey with " +
      s"$S3SslEnabledKey=false for $bucket (Hadoop addresses that endpoint over http://, " +
      "while the native S3 client always assumes https://, so a claimed scan would fail at " +
      "execution where Spark reads fine)"

  private def stsEndpointReason(bucket: String, key: String): String =
    s"Native scan does not support $key configured for $bucket (Hadoop sends its " +
      "AssumeRole request to the configured STS endpoint, while the native S3 client's " +
      "assumed-role provider uses the SDK defaults, so the two sides would authenticate " +
      "against different endpoints)"

  /**
   * First reason any bucket among `uris` configures an endpoint native would address differently
   * from Hadoop: a scheme-less `fs.s3a.endpoint` with SSL disabled, or an assumed-role STS
   * endpoint. Resolved like the keys' real consumers (plain reads on the propagated conf,
   * mirroring [[proxyGateReason]]); the STS keys decline whenever set, since dead config can
   * become live through a provider-chain change native never re-validates.
   */
  private[comet] def hadoopOnlyEndpointGateReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        try {
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          val endpoint = propagatedConf.getTrimmed(S3EndpointKey, "")
          val schemeless = endpoint.nonEmpty && !endpoint.contains("://")
          if (schemeless && !propagatedConf.getBoolean(S3SslEnabledKey, true)) {
            Some(insecureEndpointReason(bucket))
          } else if (cometProviderClassConfigured(hadoopConf, bucket)) {
            // The STS endpoint only matters to native's own assume-role provider, which the SPI
            // bridge replaces; the endpoint check above applies to the store itself.
            None
          } else {
            S3StsEndpointKeys
              .find(key => propagatedConf.getTrimmed(key, "").nonEmpty)
              .map(key => stsEndpointReason(bucket, key))
          }
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, S3EndpointKey, e))
        }
      }
    }
  }

  /**
   * `baseKey`'s long, short, then global per-bucket aliases, in Hadoop's own resolution order.
   */
  private def longThenShortThenGlobalAliases(bucket: String, baseKey: String): Seq[String] = {
    val suffix = baseKey.stripPrefix("fs.s3a.")
    Seq(s"fs.s3a.bucket.$bucket.fs.s3a.$suffix", s"fs.s3a.bucket.$bucket.$suffix", baseKey)
  }

  private def s3aScopedProviderPathReason(bucket: String, providerPathKey: String): String =
    "Native scan cannot forward Hadoop credential-provider aliases for " +
      s"$bucket ($providerPathKey configures an S3A-scoped Hadoop credential provider that " +
      "Configuration#getPassword does not consult, so the native S3 client's credentials " +
      "cannot be verified)"

  private def unverifiableCredentialProviderReason(bucket: String, error: Throwable): String =
    "Native scan cannot verify Hadoop credential-provider aliases for " +
      s"$bucket (reading $HadoopCredentialProviderPathKey raised " +
      s"${error.getClass.getName}), declining rather than risk missing credentials"

  /**
   * Three-way Hadoop credential-provider-path precheck shared by every `getPassword`-based
   * resolution below, a property of the bucket alone. An S3A- or bucket-scoped provider path,
   * which `Configuration#getPassword` never consults, yields [[UnverifiableProvider]] with no
   * keystore I/O. A global path with any entry that is not a local keystore yields
   * [[NonLocalGlobalProvider]], also with no I/O (see [[globalProviderPathHasNonLocalEntry]]).
   * Only a global path of local entries yields [[GlobalProviderOnly]], the one arm whose callers
   * do real keystore I/O and must wrap their own `getPassword` calls in try/catch. No provider
   * path anywhere yields [[NoProvider]], zero-I/O plain conf reads only.
   */
  private sealed trait CredentialProviderArm
  private case class UnverifiableProvider(offendingKey: String) extends CredentialProviderArm
  private case object NonLocalGlobalProvider extends CredentialProviderArm
  private case object GlobalProviderOnly extends CredentialProviderArm
  private case object NoProvider extends CredentialProviderArm

  /**
   * True when the global provider path has an entry other than a local keystore or a
   * filesystem-free provider. An entry passes only as `jceks`, `localjceks`, `bcfks` or
   * `localbcfks` with nested scheme `file` (as in `jceks://file/...`) while `fs.file.impl` is
   * unset or `org.apache.hadoop.fs.LocalFileSystem`, or as `user` with no authority (`user:///`).
   * Any other entry would run a third-party provider or open a filesystem during planning, and
   * S3AFileSystem drops entries whose filesystem class is S3A-compatible before it resolves
   * anything (`ProviderUtils.excludeIncompatibleCredentialProviders`), so reading one could also
   * disagree with Hadoop. An unparseable or empty entry counts as non-local. The nested scheme is
   * the part of the authority before `@`, as in `ProviderUtils.unnestUri`.
   */
  private def globalProviderPathHasNonLocalEntry(hadoopConf: Configuration): Boolean =
    Option(hadoopConf.get(HadoopCredentialProviderPathKey)).filter(_.nonEmpty).exists { path =>
      val isLocalFileSystem = Option(hadoopConf.get("fs.file.impl"))
        .forall(_ == "org.apache.hadoop.fs.LocalFileSystem")
      path.split(",").exists(entry => !isLocalOrFilesystemFreeProvider(entry, isLocalFileSystem))
    }

  private val KeystoreProviderSchemes = Set("jceks", "localjceks", "bcfks", "localbcfks")

  private def isLocalOrFilesystemFreeProvider(
      entry: String,
      isLocalFileSystem: Boolean): Boolean =
    try {
      val uri = new URI(entry)
      Option(uri.getAuthority) match {
        case Some(authority) =>
          KeystoreProviderSchemes.contains(uri.getScheme) && isLocalFileSystem &&
          authority.split("@", 2)(0) == "file"
        case None => uri.getScheme == "user"
      }
    } catch {
      case _: URISyntaxException => false
    }

  private def nonLocalProviderPathReason(bucket: String): String =
    s"Native scan cannot verify the S3 settings for $bucket " +
      s"($HadoopCredentialProviderPathKey names a credential keystore that is not on the local " +
      "filesystem, and the settings cannot be verified without reading it)"

  private def credentialProviderArm(
      hadoopConf: Configuration,
      bucket: String): CredentialProviderArm = {
    val bucketPathKey = s3aBucketProviderPathKey(bucket)
    val bucketLongPathKey = s3aBucketLongProviderPathKey(bucket)
    val s3aPathSet = nonEmptyConf(hadoopConf, S3aCredentialProviderPathKey)
    val bucketPathSet = nonEmptyConf(hadoopConf, bucketPathKey)
    val bucketLongPathSet = nonEmptyConf(hadoopConf, bucketLongPathKey)
    if (s3aPathSet || bucketPathSet || bucketLongPathSet) {
      val offendingKey =
        if (s3aPathSet) S3aCredentialProviderPathKey
        else if (bucketPathSet) bucketPathKey
        else bucketLongPathKey
      UnverifiableProvider(offendingKey)
    } else if (globalProviderPathHasNonLocalEntry(hadoopConf)) {
      NonLocalGlobalProvider
    } else if (nonEmptyConf(hadoopConf, HadoopCredentialProviderPathKey)) {
      GlobalProviderOnly
    } else {
      NoProvider
    }
  }

  /**
   * Resolves `aliases` in order under `hadoopConf`/`bucket`, keeping the first non-empty value,
   * or `Left(reason)` when the value cannot be safely verified. Dispatches on
   * [[credentialProviderArm]]: [[UnverifiableProvider]] and [[NonLocalGlobalProvider]] decline
   * with zero I/O; [[GlobalProviderOnly]] resolves each alias via `Configuration#getPassword`,
   * keystore I/O contained in try/catch so a corrupt store declines this bucket rather than
   * aborting planning; [[NoProvider]] resolves each alias via zero-I/O [[plainValue]] reads,
   * which honor [[ClearTextFallbackKey]] the way a real `getPassword` consumer would. Callers
   * such as [[hadoopLookupPasswordEffective]] and [[effectiveEncryptionAlgorithm]] supply the
   * alias lists that match their consumer's real per-tier `getPassword` calls.
   */
  private def resolveViaCredentialAliases(
      hadoopConf: Configuration,
      bucket: String,
      aliases: Seq[String]): Either[String, Option[String]] =
    credentialProviderArm(hadoopConf, bucket) match {
      case UnverifiableProvider(offendingKey) =>
        Left(s3aScopedProviderPathReason(bucket, offendingKey))
      case NonLocalGlobalProvider =>
        Left(nonLocalProviderPathReason(bucket))
      case GlobalProviderOnly =>
        try {
          Right(
            aliases.iterator
              .map(alias =>
                Option(hadoopConf.getPassword(alias)).map(new String(_)).filter(_.nonEmpty))
              .collectFirst { case Some(v) => v })
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Left(unverifiableCredentialProviderReason(bucket, e))
        }
      case NoProvider =>
        if (!hadoopConf.getBoolean(ClearTextFallbackKey, true)) {
          Right(None)
        } else {
          Right(aliases.flatMap(plainValue(hadoopConf, _)).headOption)
        }
    }

  /**
   * `bucket`'s effective value for `baseKey` in Hadoop's own `S3AUtils#lookupPassword` resolution
   * order -- long bucket alias, then short bucket alias, then global, each tried through a Hadoop
   * credential provider before falling back to plain conf (see [[resolveViaCredentialAliases]]
   * for the Arm A/B/C dispatch this delegates to) -- or `Left(reason)` when the value cannot be
   * safely verified.
   *
   * USED ONLY for keys whose real consumer IS `lookupPassword` -- the [[LookupPasswordConsumer]]
   * entries of [[S3ConfigKeyConsumers]], via [[s3KeyDivergenceReason]]. An earlier version ran
   * EVERY compared key through this function, reasoning that a wider read could only ever
   * over-decline; for a value-EQUALITY comparator that reasoning is half-true: the long-form
   * alias this function consults FIRST can hold exactly the value native resolves while Hadoop's
   * true propagate-then-plain-get value differs (e.g. a `${...}` reference whose referent
   * propagation redirects), producing a false EQUALITY that admits a really-diverging scan. A
   * [[PropagatedOptionConsumer]] key must therefore resolve like its actual consumer instead --
   * see [[s3KeyDivergenceReason]].
   *
   * Honors [[ClearTextFallbackKey]] (via [[resolveViaCredentialAliases]]'s [[NoProvider]] arm),
   * matching `getPassword`'s real refusal to read plaintext conf when the flag is off.
   *
   * `hadoopConf` must be a [[propagateBucketOptions]] result (the caller,
   * [[s3KeyDivergenceReason]], always passes one) so that `${...}` references embedded in any
   * alias resolve exactly like `S3AFileSystem#initialize`'s real propagate-then-resolve order.
   */
  private def hadoopLookupPasswordEffective(
      hadoopConf: Configuration,
      bucket: String,
      baseKey: String): Either[String, Option[String]] =
    resolveViaCredentialAliases(
      hadoopConf,
      bucket,
      longThenShortThenGlobalAliases(bucket, baseKey))

  private def effectiveValueDivergenceReason(bucket: String, key: String): String =
    s"Native scan cannot forward $key for $bucket (Hadoop's effective value for this key " +
      "differs from what the native S3 client resolves, so its credentials or configuration " +
      "would differ from Hadoop's)"

  private def unverifiableValueReason(bucket: String, key: String, error: Throwable): String =
    s"Native scan cannot verify $key for $bucket (Configuration#get raised " +
      s"${error.getClass.getName}), declining rather than risk forwarding a stale or " +
      "diverging value"

  /**
   * `None` when `baseKey`'s Hadoop-effective and native-effective values under `bucket` agree, or
   * a decline reason naming `baseKey` and `bucket` (never a value) when they diverge or either
   * side cannot be safely computed.
   *
   * Hadoop's effective value is computed against [[propagateBucketOptions]]'s result, mirroring
   * `S3AFileSystem#initialize`'s actual order (propagate bucket options into the conf FIRST, only
   * THEN read/substitute options against it), through the resolution `consumer` declares for the
   * key in [[S3ConfigKeyConsumers]]: [[LookupPasswordConsumer]] keys via
   * [[hadoopLookupPasswordEffective]] (long-then-short-then-global, keystore- and
   * [[ClearTextFallbackKey]]-aware), [[PropagatedOptionConsumer]] keys via a plain
   * `Configuration#get` on the propagated view (short-form wins by propagation alone; the long
   * form and any keystore/fallback handling are ignored, exactly like the key's real consumer).
   * Resolving a plain-consumer key through the wider `lookupPassword` cascade instead would let a
   * long-form alias value Hadoop never reads EQUAL native's resolution while Hadoop's true
   * plain-get value differs -- a false equality admitting a diverging scan, not merely an extra
   * decline. Native's effective value is always `nativeShortThenGlobal(hadoopConf, ...)` on the
   * ORIGINAL, unpropagated conf, matching `NativeConfig.extractObjectStoreOptions`'s actual
   * forwarding semantics (no propagation step) AND native's `get_config` presence-based (not
   * emptiness-based) short-vs-global fallback -- see [[nativeShortThenGlobal]]. Both values are
   * trimmed together, symmetrically, right before the equality check below (mirroring
   * `get_config_trimmed`'s `.trim()`, which native applies regardless of which alias it read)
   * rather than trimming [[nativeShortThenGlobal]]'s result on its own -- see
   * [[nativeShortThenGlobal]]'s doc for why a one-sided trim there would flag a spurious
   * divergence. Comparing against the propagated view (rather than the original conf, as an
   * earlier version of this check did) matters because propagation can change what a `${...}`
   * reference inside one bucket-scoped value resolves to: e.g.
   * `fs.s3a.bucket.B.access.key=${fs.s3a.custom.ref}` with `fs.s3a.bucket.B.custom.ref=X` and
   * global `fs.s3a.custom.ref=Y` propagates to `fs.s3a.custom.ref=X` (overwriting the global `Y`)
   * before the access key's `${...}` reference is ever substituted, so Hadoop resolves `X` while
   * a check against the unpropagated conf would (wrongly) also see `Y`, the same value native
   * forwards -- masking a real divergence. Wrapped in try/catch: `Configuration#get` raises
   * `IllegalStateException` once `${...}` substitution recurses past Hadoop's `MAX_SUBST` bound
   * (e.g. a two-key mutual reference cycle); declining is safer than crashing planning or
   * comparing a partially-substituted value.
   */
  private def s3KeyDivergenceReason(
      hadoopConf: Configuration,
      propagatedConf: Configuration,
      bucket: String,
      baseKey: String,
      consumer: S3ConfigConsumer,
      adapterView: Option[Configuration]): Option[String] = {
    def effectiveOn(conf: Configuration): Either[String, Option[String]] = consumer match {
      case LookupPasswordConsumer =>
        hadoopLookupPasswordEffective(conf, bucket, baseKey)
      case PropagatedOptionConsumer =>
        Right(Option(conf.get(baseKey)))
    }
    try {
      val hadoopEffective = effectiveOn(propagatedConf)
      hadoopEffective match {
        case Left(reason) => Some(reason)
        case Right(hadoopValue) =>
          // On an SPI bucket the value that matters is the one the SPI provider reconstructs
          // from the forwarded options (see [[spiAdapterView]]), not native's own
          // short-then-global read.
          val nativeValue = adapterView match {
            case Some(view) => effectiveOn(view).toOption.flatten
            case None => nativeShortThenGlobal(hadoopConf, bucket, baseKey)
          }
          if (hadoopValue.map(_.trim) != nativeValue.map(_.trim)) {
            Some(effectiveValueDivergenceReason(bucket, baseKey))
          } else {
            None
          }
      }
    } catch {
      case e @ (_: IOException | _: RuntimeException) =>
        Some(unverifiableValueReason(bucket, baseKey, e))
    }
  }

  /**
   * First reason any bucket among `uris` cannot faithfully forward every [[AllS3ConfigKeys]]
   * option to native, or `None` when every key's Hadoop-effective and native-effective value
   * agrees for every S3/S3A bucket referenced. Only `s3`/`s3a` authorities matter here (other
   * schemes are handled by their own callers). One comparator replaces the former per-case gate
   * family (long-form bucket credentials, JCEKS/provider shadowing, Hadoop `${...}` variable
   * references): [[s3KeyDivergenceReason]] computes Hadoop's effective value against a per-bucket
   * [[propagateBucketOptions]] replica (matching `S3AFileSystem#initialize`'s real
   * propagate-then-resolve order), so a `${...}` reference that resolves identically under that
   * propagated view and under native's unpropagated forwarding is no longer a divergence at all,
   * while one that resolves differently (e.g. because propagation shadowed a referenced key with
   * a per-bucket override) IS still caught. [[s3KeyDivergenceReason]] resolves each key through
   * the consumer family [[S3ConfigKeyConsumers]] declares for it, right beside the key itself:
   * the SSE-C long-bucket-alias bypass came from a key's resolution being decided implicitly,
   * scattered across call sites, so the classification is now a single visible list -- and the
   * tier must MATCH the key's real consumer in both directions, because an equality comparator
   * resolving a plain-get key through the wider `lookupPassword` cascade can manufacture a false
   * EQUALITY (long-form alias equal to native's value, true propagated plain value different)
   * just as readily as a false divergence. Never interpolates a resolved value, only key names.
   */
  private[comet] def s3ConfigDivergenceReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        try {
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          val adapterView = spiAdapterView(hadoopConf, propagatedConf, bucket)
          S3ConfigKeyConsumers.foldLeft(Option.empty[String]) {
            case (keyDeclined, (key, consumer)) =>
              if (keyDeclined.isDefined) {
                keyDeclined
              } else {
                s3KeyDivergenceReason(
                  hadoopConf,
                  propagatedConf,
                  bucket,
                  key,
                  consumer,
                  adapterView.filter(_ => NativeCredentialChainKeys.contains(key)))
              }
          }
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, AllS3ConfigKeys.head, e))
        }
      }
    }
  }

  /**
   * String-literal mirror of every credential-provider class name s3.rs's
   * `build_aws_credential_provider_metadata` recognizes (Hadoop S3A plus AWS SDK v1/v2 names).
   * `hadoop-aws` is NOT on this module's runtime classpath, so these stay string literals, never
   * `classOf` references.
   */
  private val SupportedCredentialProviderClasses: Set[String] = Set(
    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider",
    "com.amazonaws.auth.ContainerCredentialsProvider",
    "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper",
    "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider",
    "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    "com.amazonaws.auth.profile.ProfileCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider",
    "com.amazonaws.auth.AnonymousAWSCredentials")

  private val AnonymousCredentialProviderClasses: Set[String] = Set(
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider",
    "com.amazonaws.auth.AnonymousAWSCredentials")

  private val HadoopAssumedRoleProviderClass =
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"

  private val AwsCredentialsProviderKey = "fs.s3a.aws.credentials.provider"
  private val AssumedRoleCredentialsProviderKey = "fs.s3a.assumed.role.credentials.provider"

  /** Splits a comma-separated credential-provider-class list the same way s3.rs's parser does. */
  private def parseProviderClassNames(value: String): Seq[String] =
    value.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  // The class name is withheld: an unsupported value is never one of the known classes, and a
  // `${...}` reference can expand the key to a credential.
  private def unsupportedProviderReason(bucket: String, key: String): String =
    "Native scan does not support a credential provider class " +
      s"configured via $key for $bucket (the native S3 client only supports a fixed set of " +
      "provider classes; an unsupported class would fail at scan execution time, after the " +
      "scan was already claimed, rather than at planning time)"

  private def mixedAnonymousProviderReason(bucket: String, key: String): String =
    s"Native scan does not support $key for $bucket naming an anonymous credential " +
      "provider together with any other provider (the native S3 client rejects this " +
      "combination at scan execution time)"

  private def anonymousAssumedRoleProviderReason(bucket: String, key: String): String =
    s"Native scan does not support an anonymous credential provider in $key for " +
      s"$bucket (the native S3 client does not allow an anonymous provider as the base " +
      "credentials for an assumed-role chain)"

  private def unsupportedProviderNameReason(
      bucket: String,
      key: String,
      names: Seq[String]): Option[String] =
    if (names.exists(name => !SupportedCredentialProviderClasses.contains(name))) {
      Some(unsupportedProviderReason(bucket, key))
    } else {
      None
    }

  /**
   * [[shortThenGlobal]] for `key` under `bucket`, or `Left(reason)` when `Configuration#get`
   * itself raises: Hadoop throws `IllegalStateException` once `${...}` expansion recurses past
   * its `MAX_SUBST` bound, e.g. a mutual reference cycle between two provider keys. Every
   * provider-class read below goes through this wrapper so the exception is caught here,
   * whichever entry point runs first. Reuses [[unverifiableValueReason]]'s message shape (names
   * the key and the exception class, never a value).
   */
  private def shortThenGlobalOrReason(
      hadoopConf: Configuration,
      bucket: String,
      key: String): Either[String, Option[String]] =
    try {
      Right(shortThenGlobal(hadoopConf, bucket, key))
    } catch {
      case e @ (_: IOException | _: RuntimeException) =>
        Left(unverifiableValueReason(bucket, key, e))
    }

  /**
   * Decline reason when `bucket`'s effective `assumed.role.credentials.provider` names an
   * unsupported class, or an anonymous one (native rejects ANY anonymous entry here, not just a
   * mix), or when reading it raises (see [[shortThenGlobalOrReason]]). Unset defaults to native's
   * own always-supported fallback, so `None` is safe.
   */
  private def assumedRoleProviderClassReason(
      hadoopConf: Configuration,
      bucket: String): Option[String] = {
    shortThenGlobalOrReason(hadoopConf, bucket, AssumedRoleCredentialsProviderKey) match {
      case Left(reason) => Some(reason)
      case Right(None) => None
      case Right(Some(value)) =>
        val names = parseProviderClassNames(value)
        unsupportedProviderNameReason(bucket, AssumedRoleCredentialsProviderKey, names).orElse {
          if (names.exists(AnonymousCredentialProviderClasses.contains)) {
            Some(anonymousAssumedRoleProviderReason(bucket, AssumedRoleCredentialsProviderKey))
          } else {
            None
          }
        }
    }
  }

  /**
   * Decline reason when `bucket`'s effective `aws.credentials.provider` names an unrecognized
   * class, mixes an anonymous provider with any other, a nested `AssumedRoleCredentialProvider`
   * sub-chain has the same problem, or reading either key raises (see
   * [[shortThenGlobalOrReason]]). Unset/empty falls back to native's default chain.
   */
  private def providerClassReason(hadoopConf: Configuration, bucket: String): Option[String] = {
    shortThenGlobalOrReason(hadoopConf, bucket, AwsCredentialsProviderKey) match {
      case Left(reason) => Some(reason)
      case Right(None) => None
      case Right(Some(value)) =>
        val names = parseProviderClassNames(value)
        unsupportedProviderNameReason(bucket, AwsCredentialsProviderKey, names)
          .orElse {
            if (names.length > 1 && names.exists(AnonymousCredentialProviderClasses.contains)) {
              Some(mixedAnonymousProviderReason(bucket, AwsCredentialsProviderKey))
            } else {
              None
            }
          }
          .orElse {
            if (names.contains(HadoopAssumedRoleProviderClass)) {
              assumedRoleProviderClassReason(hadoopConf, bucket)
            } else {
              None
            }
          }
    }
  }

  /**
   * The compared keys only native's own credential chain reads: the static credentials, the
   * provider class list and the assumed-role settings. On a bucket that uses Comet's
   * credential-provider SPI native never reads them (see [[cometProviderClassConfigured]]), so
   * what matters is Hadoop's value against the SPI provider's (see [[spiAdapterView]]).
   */
  private val NativeCredentialChainKeys: Set[String] = Set(
    "fs.s3a.access.key",
    "fs.s3a.secret.key",
    "fs.s3a.session.token",
    "fs.s3a.aws.credentials.provider",
    "fs.s3a.assumed.role.arn",
    "fs.s3a.assumed.role.session.name",
    "fs.s3a.assumed.role.credentials.provider")

  /**
   * For a bucket on Comet's credential-provider SPI whose Hadoop credential resolution the SPI
   * provider can reproduce, the configuration that provider reconstructs; `None` otherwise. The
   * built-in adapters rebuild Hadoop's provider chain from the options native forwards, which are
   * the `fs.s3a.*` entries with every `${...}` reference already expanded against the ORIGINAL,
   * unpropagated conf, and then apply `propagateBucketOptions` themselves. Long-form and
   * per-bucket aliases therefore resolve as in Hadoop, but a reference whose target a bucket
   * override shadows is already pinned to the global value, so [[NativeCredentialChainKeys]] are
   * compared against this view instead of native's own read. The adapter cannot see a Hadoop
   * credential provider (a keystore path) or a disabled clear-text fallback, so a bucket with
   * either gets no view and keeps the full comparison.
   */
  private def spiAdapterView(
      hadoopConf: Configuration,
      propagatedConf: Configuration,
      bucket: String): Option[Configuration] =
    if (cometProviderClassConfigured(hadoopConf, bucket) &&
      credentialProviderArm(propagatedConf, bucket) == NoProvider &&
      propagatedConf.getBoolean(ClearTextFallbackKey, true)) {
      val forwarded = new Configuration(false)
      hadoopConf.iterator().asScala.foreach { entry =>
        val key = entry.getKey
        if (key.startsWith("fs.s3a.")) {
          val value =
            try {
              Option(hadoopConf.get(key)).getOrElse(entry.getValue)
            } catch {
              case _: IllegalStateException => entry.getValue
            }
          forwarded.set(key, value)
        }
      }
      Some(propagateBucketOptions(forwarded, bucket))
    } else {
      None
    }

  private val CometCredentialProviderClassKey = "fs.s3a.comet.credential.provider.class"

  /**
   * True when native routes `bucket`'s credentials through Comet's credential-provider SPI
   * (`CometS3CredentialBridge`) instead of building its own provider chain: s3.rs
   * `lookup_provider_class` resolves this key short-bucket-then-global and trims it, and a blank
   * value (including an empty per-bucket override) opts out. In that case native never reads
   * `fs.s3a.aws.credentials.provider`, `fs.s3a.assumed.role.credentials.provider` or
   * `fs.s3a.assumed.role.policy`, so those gates have nothing to protect: the SPI provider (for
   * the built-in adapters, Hadoop's own provider construction) resolves the identity on the JVM.
   * A read that raises reports false so the caller's gate still runs.
   */
  private def cometProviderClassConfigured(hadoopConf: Configuration, bucket: String): Boolean =
    try {
      nativeShortThenGlobal(hadoopConf, bucket, CometCredentialProviderClassKey)
        .exists(_.trim.nonEmpty)
    } catch {
      case _: IllegalStateException => false
    }

  /**
   * First reason any bucket among `uris` names an unsupported credential-provider class, or
   * `None` when every named class is supported (or the key is unset). `NativeConfig` forwards
   * `Configuration#get`'s substituted value for every entry, the same read [[shortThenGlobal]]
   * performs here, so a `${...}` reference resolves identically for native and for this check.
   */
  private[comet] def providerClassGateReason(
      hadoopConf: Configuration,
      uris: Seq[URI]): Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined || cometProviderClassConfigured(hadoopConf, bucket)) {
        declined
      } else {
        providerClassReason(hadoopConf, bucket)
      }
    }
  }
}

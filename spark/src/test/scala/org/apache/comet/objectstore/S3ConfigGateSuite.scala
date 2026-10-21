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

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import org.scalatest.funsuite.AnyFunSuite

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.hadoop.fs.s3a.{S3AFileSystem, S3AUtils}
import org.apache.hadoop.security.alias.CredentialProviderFactory

/**
 * Pins [[S3ConfigGate]], the check that declines a native scan when Comet's native S3 client
 * would resolve a different S3 configuration than Hadoop's `S3AFileSystem`. Every case compares
 * the gate against real Hadoop code (`S3AUtils`, real JCEKS keystores) rather than a stand-in.
 */
class S3ConfigGateSuite extends AnyFunSuite {

  /**
   * A Hadoop `Configuration` variable reference to `key`, as `Configuration#get` expands it.
   * Built by concatenation: written as a literal, it reads to scalac as a string missing its `s`
   * interpolator, and adding the `s` (escaping the dollar) is then flagged by scalafix as
   * redundant.
   */
  private def varRef(key: String): String = "${" + key + "}"

  // The build points java.io.tmpdir under target/, which a fresh checkout may not have yet; the
  // keystore and temp-directory fixtures below create their files there.
  Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))

  /**
   * Builds a real JCEKS keystore backing `hadoop.security.credential.provider.path`, seeded with
   * `entries`, and hands `test` a fresh [[Configuration]] already pointed at it (path only --
   * `entries` are NOT mirrored into the plain conf; callers add plain values themselves when a
   * case needs them). Uses `CredentialProviderFactory` directly (the real API `Configuration#
   * getPassword` reads through), not a hand-rolled keystore, so these tests exercise the actual
   * Hadoop credential-provider resolution path rather than a stand-in for it. The store password
   * defaults to `"none"` when neither `HADOOP_CREDSTORE_PASSWORD` nor a password file is set in
   * the test environment, which is the JCEKS provider's own documented default -- nothing extra
   * to configure here.
   */
  private def withJceks(entries: Map[String, String])(test: Configuration => Unit): Unit = {
    val storeFile = File.createTempFile("comet-s3-creds", ".jceks")
    // JavaKeyStoreProvider creates the backing file itself on first flush; a pre-existing empty
    // file (createTempFile always creates one) makes it treat the store as an existing, empty
    // keystore instead -- harmless either way for JCEKS, but deleting it first keeps this fixture
    // honest about what it is actually exercising (provider-created, not merely provider-opened).
    storeFile.delete()
    val providerPath = "jceks://file" + storeFile.getAbsolutePath
    try {
      val buildConf = new Configuration(false)
      buildConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      val provider = CredentialProviderFactory.getProviders(buildConf).get(0)
      entries.foreach { case (alias, value) =>
        provider.createCredentialEntry(alias, value.toCharArray)
      }
      provider.flush()

      val testConf = new Configuration(false)
      testConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      test(testConf)
    } finally {
      storeFile.delete()
    }
  }

  test(
    "s3ConfigDivergenceReason declines when access/secret keys exist only in a JCEKS " +
      "keystore, naming the base key and bucket but never the secret") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAEXAMPLE", "fs.s3a.secret.key" -> "s3cr3tValue")) {
      conf =>
        val reason = S3ConfigGate.s3ConfigDivergenceReason(
          conf,
          Seq(new URI("s3a://mybucket/part-0.parquet")))
        assert(reason.isDefined)
        assert(reason.get.contains("fs.s3a.access.key"))
        assert(reason.get.contains("mybucket"))
        assert(!reason.get.contains("AKIAEXAMPLE"))
        assert(!reason.get.contains("s3cr3tValue"))
    }
  }

  test(
    "s3ConfigDivergenceReason passes when only plain keys are set and no provider path is " +
      "configured (zero-I/O precheck exit)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIAPLAIN")
    conf.set("fs.s3a.secret.key", "plainSecret")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when the provider path is set and the plain keys match " +
      "the keystore (plain keys consistent with the credential provider)") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAMATCH", "fs.s3a.secret.key" -> "matchingSecret")) {
      conf =>
        conf.set("fs.s3a.access.key", "AKIAMATCH")
        conf.set("fs.s3a.secret.key", "matchingSecret")
        assert(
          S3ConfigGate
            .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
            .isEmpty)
    }
  }

  test(
    "s3ConfigDivergenceReason declines when the keystore value differs from a shadowed plain " +
      "value") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAKEYSTORE")) { conf =>
      conf.set("fs.s3a.access.key", "AKIADIFFERENTPLAIN")
      val reason =
        S3ConfigGate.s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(!reason.get.contains("AKIAKEYSTORE"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines on an S3A-scoped provider path immediately, without " +
      "touching a nonexistent keystore (Arm A proves no keystore I/O)") {
    val tempDir = Files.createTempDirectory("comet-s3-no-keystore")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      // No exception from a missing file is the point of this test: Arm A declines on the
      // presence of the S3A-scoped path key alone, never reading it.
      val reason =
        S3ConfigGate.s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.security.credential.provider.path"))
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "s3ConfigDivergenceReason passes file:// URIs regardless of any provider path " +
      "(S3-only scope)") {
    val conf = new Configuration(false)
    conf.set("hadoop.security.credential.provider.path", "jceks://file/nonexistent.jceks")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("file:///tmp/table/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines via a per-bucket credential alias " +
      "(fs.s3a.bucket.mybucket.access.key)") {
    withJceks(Map("fs.s3a.bucket.mybucket.access.key" -> "AKIABUCKETSCOPED")) { conf =>
      val reason =
        S3ConfigGate.s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("AKIABUCKETSCOPED"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines via a long-form per-bucket credential alias " +
      "(fs.s3a.bucket.mybucket.fs.s3a.access.key), a Hadoop S3AUtils.lookupPassword alias " +
      "the short-form check alone misses") {
    withJceks(
      Map(
        "fs.s3a.bucket.mybucket.fs.s3a.access.key" -> "AKIALONGFORM",
        "fs.s3a.bucket.mybucket.fs.s3a.secret.key" -> "longFormSecret")) { conf =>
      val reason =
        S3ConfigGate.s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("AKIALONGFORM"))
      assert(!reason.get.contains("longFormSecret"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines via a long-form per-bucket credential alias even when " +
      "different plain global keys are also configured (Hadoop would resolve the long-form " +
      "keystore value first; native reads only the differing plain globals)") {
    withJceks(
      Map(
        "fs.s3a.bucket.mybucket.fs.s3a.access.key" -> "AKIALONGFORM",
        "fs.s3a.bucket.mybucket.fs.s3a.secret.key" -> "longFormSecret")) { conf =>
      conf.set("fs.s3a.access.key", "AKIADIFFERENTGLOBAL")
      conf.set("fs.s3a.secret.key", "differentGlobalSecret")
      val reason =
        S3ConfigGate.s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("AKIALONGFORM"))
      assert(!reason.get.contains("longFormSecret"))
      assert(!reason.get.contains("AKIADIFFERENTGLOBAL"))
      assert(!reason.get.contains("differentGlobalSecret"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines on a long-form per-bucket provider path immediately, " +
      "without touching a nonexistent keystore (Arm A proves no keystore I/O)") {
    val tempDir = Files.createTempDirectory("comet-s3-no-keystore-long-bucket")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path", nonexistentPath)
      // No exception from a missing file is the point of this test: Arm A declines on the
      // presence of the long-form bucket-scoped path key alone, never reading it.
      val reason =
        S3ConfigGate.s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(
        reason.get.contains("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path"))
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "s3ConfigDivergenceReason passes when only plain global keys are set and no provider " +
      "path is configured, including the long-form bucket provider path (control: unaffected " +
      "by the new long-form aliases)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIAPLAIN")
    conf.set("fs.s3a.secret.key", "plainSecret")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines without throwing when the keystore is " +
      "corrupt/unreadable (global arm try/catch containment)") {
    val corruptFile = File.createTempFile("comet-s3-corrupt-creds", ".jceks")
    try {
      Files.write(corruptFile.toPath, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
      val conf = new Configuration(false)
      conf.set(
        "hadoop.security.credential.provider.path",
        "jceks://file" + corruptFile.getAbsolutePath)
      // Must not throw: a corrupt/unreadable keystore must decline this bucket, not escape and
      // abort planning for the whole session.
      val reason =
        S3ConfigGate.s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
    } finally {
      corruptFile.delete()
    }
  }

  test(
    "s3ConfigDivergenceReason declines when a plain long-form bucket credential key is set " +
      "with nothing else (Hadoop resolves it, native's short-then-global lookup never sees " +
      "it), naming the base key and bucket but never a credential value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIALONGPLAIN")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "longPlainSecret")
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("AKIALONGPLAIN"))
    assert(!reason.get.contains("longPlainSecret"))
  }

  test("s3ConfigDivergenceReason declines when a long-form bucket credential holds a Hadoop " +
    s"${varRef("...")} reference that DOES resolve, with nothing else set (substitution alone does not " +
    "erase the long-form divergence: native's short-then-global read never consults the long " +
    "form regardless of what it expands to), naming the base key and bucket but never a value") {
    val conf = new Configuration(false)
    conf.set("review.longFormAccess", "AKIALONGRESOLVED")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", varRef("review.longFormAccess"))
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("AKIALONGRESOLVED"))
    assert(!reason.get.contains(varRef("review.longFormAccess")))
  }

  test(
    "s3ConfigDivergenceReason declines when a plain long-form bucket credential diverges " +
      "from a different plain global value (Hadoop would use the long-form bucket value; " +
      "native would use the differing global)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIALONGPLAIN")
    conf.set("fs.s3a.access.key", "AKIADIFFERENTGLOBALPLAIN")
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("AKIALONGPLAIN"))
    assert(!reason.get.contains("AKIADIFFERENTGLOBALPLAIN"))
  }

  test(
    "s3ConfigDivergenceReason declines when the plain long-form and short-form bucket " +
      "credential keys are set to DIFFERENT values (Hadoop's SimpleAWSCredentialsProvider " +
      "resolves the long pair; native resolves the short pair, so they diverge), naming the " +
      "base key and bucket but never a credential value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "long-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "long-sk")
    conf.set("fs.s3a.bucket.mybucket.access.key", "short-ak")
    conf.set("fs.s3a.bucket.mybucket.secret.key", "short-sk")
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("long-ak"))
    assert(!reason.get.contains("short-ak"))
  }

  test(
    "control: S3AUtils#propagateBucketOptions folds a long-form bucket option into the " +
      "unread key fs.s3a.fs.s3a.endpoint, proving Hadoop itself ignores the long form for " +
      "general (non-credential) per-bucket options -- unlike lookupPassword for credentials, " +
      "no Comet gate exists (or is needed) for this case") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.endpoint", "long-form.example.com")
    conf.set("fs.s3a.endpoint", "global.example.com")

    // Real Hadoop code, not a Comet stand-in: S3AFileSystem#initialize assigns exactly this
    // result to the `conf` it reads ENDPOINT/PATH_STYLE_ACCESS/etc. from.
    val propagated = S3AUtils.propagateBucketOptions(conf, "mybucket")
    assert(propagated.get("fs.s3a.endpoint") == "global.example.com")
    assert(propagated.get("fs.s3a.fs.s3a.endpoint") == "long-form.example.com")
  }

  test(
    "s3ConfigDivergenceReason declines when a bucket-scoped credential references another " +
      "bucket-scoped key that Hadoop's real propagate-then-resolve order shadows the global " +
      "value with (Hadoop resolves the bucket-scoped referent; native, which never propagates " +
      "bucket options, still resolves the global one), naming the base key and bucket but " +
      "never a credential value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.access.key", varRef("fs.s3a.custom.ref"))
    conf.set("fs.s3a.bucket.mybucket.custom.ref", "bucket-scoped-value")
    conf.set("fs.s3a.custom.ref", "global-value")

    // Real Hadoop code, not a Comet stand-in: this is exactly what S3AFileSystem#initialize
    // assigns to the `conf` it later reads fs.s3a.access.key from -- the bucket-scoped
    // fs.s3a.bucket.mybucket.custom.ref overwrites the global fs.s3a.custom.ref BEFORE the
    // ${...} reference in the propagated fs.s3a.bucket.mybucket.access.key is ever substituted.
    val propagated = S3AUtils.propagateBucketOptions(conf, "mybucket")
    assert(propagated.get("fs.s3a.custom.ref") == "bucket-scoped-value")
    assert(propagated.get("fs.s3a.bucket.mybucket.access.key") == "bucket-scoped-value")

    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("bucket-scoped-value"))
    assert(!reason.get.contains("global-value"))
  }

  test(
    "s3ConfigDivergenceReason passes when a bucket-scoped credential references another " +
      "bucket-scoped key whose propagated value happens to equal the global value (no actual " +
      "divergence, despite the same shadowing mechanism as the declining case above)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.access.key", varRef("fs.s3a.custom.ref"))
    conf.set("fs.s3a.bucket.mybucket.custom.ref", "same-value")
    conf.set("fs.s3a.custom.ref", "same-value")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines, without any keystore I/O, when a bucket-scoped " +
      s"long-form credential-provider-path key (Arm A) is itself set via a ${varRef("...")} reference " +
      "to another bucket-scoped key that Hadoop's real propagate-then-resolve order shadows " +
      "the global value with -- naming only the provider path key and bucket, never either " +
      "resolved path") {
    // Uses the LONG form (fs.s3a.bucket.B.fs.s3a.security.credential.provider.path), not the
    // short form, deliberately: propagateBucketOptions folds ANY fs.s3a.bucket.B.<rest> key into
    // a global fs.s3a.<rest> key. For the short form, <rest> is
    // "security.credential.provider.path", so it propagates into the GLOBAL S3A-scoped provider
    // path key itself (fs.s3a.security.credential.provider.path) -- correctly triggering the
    // OTHER Arm A branch instead, since real Hadoop would see the same thing. The long form's
    // <rest> is "fs.s3a.security.credential.provider.path", which propagates into the inert,
    // double-prefixed fs.s3a.fs.s3a.security.credential.provider.path key instead, isolating
    // the long-form bucket-scoped branch this test targets.
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path",
      varRef("fs.s3a.custom.ref"))
    conf.set(
      "fs.s3a.bucket.mybucket.custom.ref",
      "jceks://file/does-not-exist-bucket-scoped.jceks")
    conf.set("fs.s3a.custom.ref", "jceks://file/does-not-exist-global.jceks")

    // Real Hadoop code, not a Comet stand-in: this is exactly what S3AFileSystem#initialize
    // assigns to the `conf` it later reads the bucket-scoped provider path from -- the
    // bucket-scoped fs.s3a.bucket.mybucket.custom.ref overwrites the global fs.s3a.custom.ref
    // BEFORE the ${...} reference in the propagated provider path key is ever substituted.
    val propagated = S3AUtils.propagateBucketOptions(conf, "mybucket")
    assert(
      propagated.get("fs.s3a.custom.ref") == "jceks://file/does-not-exist-bucket-scoped.jceks")
    assert(
      propagated.get("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path") ==
        "jceks://file/does-not-exist-bucket-scoped.jceks")
    // Confirms the long form's propagated target is the inert double-prefixed key, NOT the
    // global S3A-scoped provider path key -- i.e. this test genuinely isolates the long-form
    // bucket-scoped branch rather than accidentally exercising the global-S3A-path branch.
    assert(propagated.get("fs.s3a.security.credential.provider.path") == null)

    // Neither referenced path exists on disk -- if this gate mistakenly tried to open either
    // as a keystore instead of declining on the key's mere presence (Arm A), it would throw
    // rather than return a reason, which this test would catch.
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("does-not-exist-bucket-scoped"))
    assert(!reason.get.contains("does-not-exist-global"))
  }

  test(
    "s3ConfigDivergenceReason declines when the long-form and global bucket credential keys " +
      "share the same value but the short-form bucket keys are set to EMPTY strings (Hadoop's " +
      "SimpleAWSCredentialsProvider resolves the long pair via lookupPassword's skip-empty " +
      "semantics; native's get_config_trimmed resolves the short pair's mere PRESENCE, landing " +
      "on empty credentials instead), naming the base key and bucket but never a credential " +
      "value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.bucket.mybucket.access.key", "")
    conf.set("fs.s3a.bucket.mybucket.secret.key", "")
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("shared-ak"))
  }

  test("s3ConfigDivergenceReason declines when the short-form bucket credential keys hold only " +
    "whitespace: native's get_config_trimmed still resolves the key's mere PRESENCE before " +
    "trimming its value, so a whitespace-only short-form key diverges from Hadoop's long-form " +
    "resolution exactly like an outright empty one") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.bucket.mybucket.access.key", "   ")
    conf.set("fs.s3a.bucket.mybucket.secret.key", "   ")
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "control: s3ConfigDivergenceReason passes when the short-form bucket credential keys are " +
      "absent rather than empty, so Hadoop's long-form resolution and native's short-then-global " +
      "resolution both land on the same shared pair") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.secret.key", "shared-sk")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "control: s3ConfigDivergenceReason passes when a global-only value (no bucket override at " +
      "all, so Hadoop's and native's effective values are the exact same conf entry) carries " +
      "incidental leading/trailing whitespace, such as Hadoop's own multi-line " +
      "fs.s3a.aws.credentials.provider default -- trimming must apply symmetrically to both " +
      "sides of the comparison, or an untouched default value would diverge from itself and " +
      "decline every S3 scan") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "\n  org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,\n  " +
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\n  ")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    s"s3ConfigDivergenceReason declines when a per-bucket override redirects the ${varRef("...")} " +
      "reference inside the short-form bucket endpoint while a long-form endpoint alias holds " +
      "the value native resolves (Hadoop's endpoint consumer is propagateBucketOptions plus " +
      "plain Configuration#get, which follows the redirected reference and never reads the " +
      "long form at all)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.custom.ref", "https://store-a.example")
    conf.set("fs.s3a.bucket.data-bucket.custom.ref", "https://store-b.example")
    conf.set("fs.s3a.bucket.data-bucket.endpoint", varRef("fs.s3a.custom.ref"))
    conf.set("fs.s3a.bucket.data-bucket.fs.s3a.endpoint", "https://store-a.example")

    // Real Hadoop code, not a Comet stand-in: propagation overwrites the global referent with
    // the per-bucket custom.ref BEFORE the endpoint's ${...} reference is substituted, so
    // Hadoop's plain endpoint read lands on store-b -- while native, which never propagates,
    // expands the same reference against the original conf and lands on store-a.
    val propagated = S3AUtils.propagateBucketOptions(conf, "data-bucket")
    assert(propagated.get("fs.s3a.endpoint") == "https://store-b.example")

    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://data-bucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.endpoint"))
    assert(reason.get.contains("data-bucket"))
    assert(!reason.get.contains("store-a"))
    assert(!reason.get.contains("store-b"))
  }

  test(
    "control: s3ConfigDivergenceReason passes the same endpoint shape without the per-bucket " +
      s"referent override (the ${varRef("...")} reference expands identically with and without " +
      "bucket-option propagation, so Hadoop's plain-get endpoint read and native agree)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.custom.ref", "https://store-a.example")
    conf.set("fs.s3a.bucket.data-bucket.endpoint", varRef("fs.s3a.custom.ref"))
    conf.set("fs.s3a.bucket.data-bucket.fs.s3a.endpoint", "https://store-a.example")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://data-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when ONLY the long-form bucket endpoint alias is set: " +
      "propagateBucketOptions folds it into the unread fs.s3a.fs.s3a.endpoint key, so Hadoop's " +
      "plain endpoint read and native's short-then-global read both resolve nothing") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.data-bucket.fs.s3a.endpoint", "https://store-a.example")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://data-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines an obsolete plaintext credential pair when " +
      "hadoop.security.credential.clear-text-fallback is false: Configuration#getPassword " +
      "ignores plain conf then, so Hadoop's SimpleAWSCredentialsProvider reports no " +
      "credentials and the chain proceeds to the environment -- while native would sign every " +
      "request with the stale static pair") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "stale-ak")
    conf.set("fs.s3a.secret.key", "stale-sk")
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    conf.set("hadoop.security.credential.clear-text-fallback", "false")
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("stale-ak"))
    assert(!reason.get.contains("stale-sk"))
  }

  test(
    "control: s3ConfigDivergenceReason passes the same plaintext pair and provider chain when " +
      "clear-text-fallback keeps its default (true): getPassword falls back to plain conf, so " +
      "Hadoop and native resolve the identical static pair") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "stale-ak")
    conf.set("fs.s3a.secret.key", "stale-sk")
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes with clear-text-fallback=false when no plaintext " +
      "credential is set anywhere: both sides resolve no credentials, and the provider-class " +
      "key itself stays comparable (its consumer is Configuration#getClasses, plain conf, " +
      "which the fallback flag never gates)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    conf.set("hadoop.security.credential.clear-text-fallback", "false")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  // -----------------------------------------------------------------------------------------
  // providerClassGateReason: declines an unsupported credential-provider class (or
  // invalid combination) before the scan is claimed, rather than letting it fail during
  // execution in s3.rs's build_aws_credential_provider_metadata.
  // -----------------------------------------------------------------------------------------

  private val nativeSupportedProviderClasses = Seq(
    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
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

  test(
    "providerClassGateReason passes when aws.credentials.provider is unset (native's " +
      "default AWS SDK provider chain)") {
    val conf = new Configuration(false)
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test("providerClassGateReason passes for every credential provider class s3.rs supports") {
    nativeSupportedProviderClasses.foreach { className =>
      val conf = new Configuration(false)
      conf.set("fs.s3a.aws.credentials.provider", className)
      val reason =
        S3ConfigGate
          .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isEmpty, s"Expected $className to be claimable, but got: $reason")
    }
  }

  test(
    "providerClassGateReason declines an unsupported credential provider class, naming the " +
      "key and the bucket but never the configured value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.aws.credentials.provider", "com.example.CustomCredentialsProvider")
    val reason =
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.aws.credentials.provider"))
    assert(!reason.get.contains("com.example.CustomCredentialsProvider"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "providerClassGateReason declines via the per-bucket short form, honoring bucket-scoped " +
      "override (mirrors get_config's short-then-global resolution)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set(
      "fs.s3a.bucket.mybucket.aws.credentials.provider",
      "com.example.CustomCredentialsProvider")
    val reason =
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(!reason.get.contains("com.example.CustomCredentialsProvider"))
  }

  test(
    "providerClassGateReason passes a comma-separated list of entirely supported provider " +
      "classes (native chains them via build_chained_aws_credential_provider_metadata)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider, " +
        "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider")
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason declines a comma-separated list containing one unsupported " +
      "class, naming neither class") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.example.Bogus")
    val reason =
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(!reason.get.contains("com.example.Bogus"))
    assert(!reason.get.contains("SimpleAWSCredentialsProvider"))
  }

  test(
    "providerClassGateReason declines an anonymous provider mixed with another provider " +
      "(native's build_credential_provider rejects this combination at execution time)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider," +
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    val reason =
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("anonymous"))
  }

  test(
    "providerClassGateReason passes a solo anonymous provider (native returns None -- an " +
      "unsigned client -- rather than erroring; only a MIX with other providers is rejected)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes AssumedRoleCredentialProvider with an unset " +
      "assumed.role.credentials.provider (native defaults to its own always-supported " +
      "[Simple, EnvironmentVariable] fallback)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason declines AssumedRoleCredentialProvider whose " +
      "assumed.role.credentials.provider names an unsupported base provider class") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    conf.set("fs.s3a.assumed.role.credentials.provider", "com.example.BogusBaseProvider")
    val reason =
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(!reason.get.contains("com.example.BogusBaseProvider"))
    assert(reason.get.contains("fs.s3a.assumed.role.credentials.provider"))
  }

  test(
    "providerClassGateReason declines AssumedRoleCredentialProvider whose " +
      "assumed.role.credentials.provider names an anonymous base provider (native rejects ANY " +
      "anonymous entry here, not just a mix)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    conf.set(
      "fs.s3a.assumed.role.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    val reason =
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("anonymous"))
  }

  test(
    "assumedRolePolicyGateReason declines when a global assumed-role session policy is " +
      "configured") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.assumed.role.policy", """{"Version":"2012-10-17","Statement":[]}""")
    val reason =
      S3ConfigGate
        .assumedRolePolicyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.assumed.role.policy"))
    // The policy document itself is security-sensitive configuration; never leak it.
    assert(!reason.get.contains("2012-10-17"))
  }

  test(
    "assumedRolePolicyGateReason declines when a bucket-scoped assumed-role session policy " +
      "is configured") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.bucket.mybucket.assumed.role.policy",
      """{"Version":"2012-10-17","Statement":[]}""")
    val reason =
      S3ConfigGate
        .assumedRolePolicyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.assumed.role.policy"))
    assert(!reason.get.contains("2012-10-17"))
  }

  test("assumedRolePolicyGateReason admits when no assumed-role session policy is configured") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/reader")
    assert(
      S3ConfigGate
        .assumedRolePolicyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason ignores assumed.role.credentials.provider when " +
      "AssumedRoleCredentialProvider is not itself in play (dead config on the native side)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.assumed.role.credentials.provider", "com.example.BogusBaseProvider")
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes when the global aws.credentials.provider key holds a " +
      "Hadoop variable reference that Configuration#get expands to a supported class " +
      "(post-substitution, native's plain-conf extraction sees the same expanded class name " +
      "the class-support check does, so no divergence exists to decline)") {
    val conf = new Configuration(false)
    conf.set("review.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.aws.credentials.provider", varRef("review.provider"))
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes when a bucket-scoped short-form " +
      "aws.credentials.provider override holds a variable reference that expands to a " +
      "supported class, even though the global key is a different supported literal") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("review.bucketProvider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.bucket.mybucket.aws.credentials.provider", varRef("review.bucketProvider"))
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes when the assumed-role base-provider key holds a " +
      "variable reference that expands to a supported base class") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    conf.set("review.baseProvider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.assumed.role.credentials.provider", varRef("review.baseProvider"))
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes literal provider classes with no variable references " +
      "(unaffected by variable expansion, still runs the class-support gate)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    assert(
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)

    val badConf = new Configuration(false)
    badConf.set("fs.s3a.aws.credentials.provider", "com.example.CustomCredentialsProvider")
    val reason =
      S3ConfigGate
        .providerClassGateReason(badConf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(!reason.get.contains("com.example.CustomCredentialsProvider"))
  }

  test(
    "providerClassGateReason declines rather than throws when a two-key mutual Hadoop " +
      "variable-reference cycle involves fs.s3a.aws.credentials.provider, called DIRECTLY " +
      "(not routed through s3ConfigDivergenceReason, which masks this for the same keys when " +
      "checked first -- this pins the gate's OWN containment, not that coupling)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      varRef("fs.s3a.assumed.role.credentials.provider"))
    conf.set(
      "fs.s3a.assumed.role.credentials.provider",
      varRef("fs.s3a.aws.credentials.provider"))
    val reason =
      S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.aws.credentials.provider"))
    assert(reason.get.contains("IllegalStateException"))
    assert(!reason.get.contains(varRef("fs.s3a.assumed.role.credentials.provider")))
    assert(!reason.get.contains(varRef("fs.s3a.aws.credentials.provider")))
  }

  test(
    "s3ConfigDivergenceReason passes when both the plain long-form and short-form bucket " +
      "credential keys are set to the EQUAL value (Hadoop's long-first resolution and " +
      "native's short-then-global resolution agree)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIASAMEBOTH")
    conf.set("fs.s3a.bucket.mybucket.access.key", "AKIASAMEBOTH")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when the plain long-form bucket credential value equals " +
      "the plain global value (both sides resolve to the same value)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIASAME")
    conf.set("fs.s3a.access.key", "AKIASAME")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes with only a plain short-form bucket credential key set " +
      "(control: unaffected by the long-form plain-value check)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.access.key", "AKIASHORTONLY")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when a credential key holds a Hadoop variable reference " +
      "that Configuration#get expands to a literal (post-substitution, native's plain-conf " +
      "extraction forwards the SAME expanded value this comparator reads, so both sides agree)") {
    val conf = new Configuration(false)
    conf.set("review.access", "AKIAEXAMPLE")
    conf.set("fs.s3a.access.key", varRef("review.access"))
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when credential keys hold literal values with no " +
      "variable references") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIALITERAL")
    conf.set("fs.s3a.secret.key", "literalSecretValue")
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when a credential key references an undefined variable " +
      "(Hadoop leaves the literal unresolved, so native and Hadoop see the identical value)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", varRef("undefined.var"))
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when a bucket-scoped short-form credential alias holds " +
      "a variable reference that expands identically for both sides (alias-set coverage " +
      "beyond the plain global key)") {
    val conf = new Configuration(false)
    conf.set("review.secret", "topSecretValue")
    conf.set("fs.s3a.bucket.mybucket.secret.key", varRef("review.secret"))
    assert(
      S3ConfigGate
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason does not throw for a credential key that is its own Hadoop " +
      "variable reference (Configuration#get's substitution loop converges immediately -- " +
      "the raw and expanded literals are already equal -- so this is the same safe shape as " +
      "an undefined variable, not a MAX_SUBST failure)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.secret.key", "realSecretValue")
    conf.set("fs.s3a.access.key", varRef("fs.s3a.access.key"))
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(
      reason.isEmpty,
      "expected no decline (and no exception) for a literal " +
        s"self-reference, since it resolves to the same unexpanded text on both sides: $reason")
  }

  test(
    "s3ConfigDivergenceReason declines rather than throws when two credential keys form a " +
      "mutual Hadoop variable-reference cycle (Configuration#get raises IllegalStateException " +
      s"once ${varRef("...")} substitution recurses past Hadoop's MAX_SUBST bound)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", varRef("fs.s3a.secret.key"))
    conf.set("fs.s3a.secret.key", varRef("fs.s3a.access.key"))
    val reason = S3ConfigGate
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("IllegalStateException"))
    assert(!reason.get.contains("realSecretValue"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines a bucket configured with global SSE-C, " +
      "naming the algorithm key and the algorithm but never the customer-provided key value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SSE-C")
    conf.set("fs.s3a.encryption.key", "c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ==")
    val reason = S3ConfigGate
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.encryption.algorithm"))
    assert(reason.get.contains("SSE-C"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ=="))
  }

  test(
    "unsupportedEncryptionAlgorithmReason matches the SSE-C algorithm value " +
      "case-insensitively, mirroring S3AEncryptionMethods#getMethod's equalsIgnoreCase " +
      "parsing") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "sse-c")
    val reason = S3ConfigGate
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("mybucket"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines a bucket configured with the deprecated " +
      "fs.s3a.server-side-encryption-algorithm spelling of SSE-C, naming the algorithm key " +
      "actually consulted but never the customer-provided key value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-C")
    conf.set("fs.s3a.server-side-encryption.key", "c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ==")
    val reason = S3ConfigGate
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    // Names EITHER spelling, never both/neither: hadoop-aws's S3AFileSystem statically registers
    // this exact pair as a Configuration-level deprecated alias (verified via javap --
    // S3AFileSystem.addDeprecatedKeys() calls Configuration.addDeprecations, a field static on
    // Hadoop's Configuration class, process-wide once S3AFileSystem's class has loaded anywhere
    // in this JVM -- which a real Spark job has always done by the time it evaluates this gate,
    // since reading the S3 table at all requires that class). Once registered,
    // Configuration#get resolves either literal key to the same value transparently, so which
    // name THIS gate happens to read the value under depends on whether that static
    // registration already ran elsewhere in the test JVM, not on anything this test controls.
    assert(
      reason.get.contains("fs.s3a.server-side-encryption-algorithm") ||
        reason.get.contains("fs.s3a.encryption.algorithm"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ=="))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines only the bucket whose per-bucket " +
      "SHORT-form key sets SSE-C, leaving an unrelated bucket unaffected") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.secure-bucket.encryption.algorithm", "SSE-C")
    val declined = S3ConfigGate.unsupportedEncryptionAlgorithmReason(
      conf,
      Seq(new URI("s3a://secure-bucket/part-0.parquet")))
    assert(declined.isDefined)
    assert(declined.get.contains("secure-bucket"))

    assert(
      S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(
          conf,
          Seq(new URI("s3a://other-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "unsupportedEncryptionAlgorithmReason DOES fire for SSE-C set only via the LONG " +
      "per-bucket form: S3AUtils#lookupBucketSecret is long-then-short, " +
      "decompiled from hadoop-aws 3.3.4's S3AUtils.class -- unlike a plain propagated option, " +
      "the encryption algorithm's bucket tier DOES consult fs.s3a.bucket.B.fs.s3a.encryption." +
      "algorithm, and Hadoop's own reader picks SSE-C from it, so this must decline exactly " +
      "like the short-form case above") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.secure-bucket.fs.s3a.encryption.algorithm", "SSE-C")
    val reason = S3ConfigGate.unsupportedEncryptionAlgorithmReason(
      conf,
      Seq(new URI("s3a://secure-bucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("secure-bucket"))
    assert(reason.get.contains("SSE-C"))
  }

  private val EncryptionAlgorithmKeys =
    Seq("fs.s3a.encryption.algorithm", "fs.s3a.server-side-encryption-algorithm")

  private val DataBucketUris = Seq(new URI("s3a://data-bucket/part-0.parquet"))

  /** The algorithm Hadoop's own S3AFileSystem selects for `bucket`: propagate, then resolve. */
  private def hadoopEncryptionAlgorithm(conf: Configuration, bucket: String): String = {
    // S3AFileSystem's static initializer registers the deprecated algorithm key as an alias of
    // the canonical one; a real filesystem always has it loaded before it resolves anything.
    // The alias is process-wide, so afterwards setting the deprecated key also stores the
    // canonical one and the deprecated-key iterations mostly re-test the canonical path.
    new S3AFileSystem()
    S3AUtils
      .getEncryptionAlgorithm(bucket, S3AUtils.propagateBucketOptions(conf, bucket))
      .getMethod
  }

  /** An algorithm key set to `${fs.s3a.custom.ref}`, with a global and optional bucket ref. */
  private def referencedAlgorithmConf(
      algorithmKey: String,
      globalRef: String,
      bucketRef: Option[String]): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.custom.ref", globalRef)
    bucketRef.foreach(conf.set("fs.s3a.bucket.data-bucket.custom.ref", _))
    conf.set(algorithmKey, varRef("fs.s3a.custom.ref"))
    conf
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines SSE-C that a bucket override selects " +
      s"through a ${varRef("...")} reference, resolving the algorithm on the propagated view as Hadoop " +
      "does, and never names the customer key") {
    val customerKey = "c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ=="
    for (algorithmKey <- EncryptionAlgorithmKeys) {
      val conf = referencedAlgorithmConf(algorithmKey, "AES256", Some("SSE-C"))
      conf.set("fs.s3a.encryption.key", customerKey)
      assert(hadoopEncryptionAlgorithm(conf, "data-bucket") == "SSE-C")

      val reason = S3ConfigGate.unsupportedEncryptionAlgorithmReason(conf, DataBucketUris)
      assert(reason.isDefined, s"expected a decline for $algorithmKey")
      assert(reason.get.contains("SSE-C"))
      assert(reason.get.contains("data-bucket"))
      assert(!reason.get.contains(customerKey))
    }
  }

  test(
    s"unsupportedEncryptionAlgorithmReason passes AES256 selected through a ${varRef("...")} reference " +
      "when no bucket override redirects it") {
    for (algorithmKey <- EncryptionAlgorithmKeys) {
      val conf = referencedAlgorithmConf(algorithmKey, "AES256", None)
      assert(hadoopEncryptionAlgorithm(conf, "data-bucket") == "AES256")
      assert(
        S3ConfigGate.unsupportedEncryptionAlgorithmReason(conf, DataBucketUris).isEmpty,
        s"expected $algorithmKey to pass")
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason passes SSE-KMS that a bucket override selects " +
      s"through a ${varRef("...")} reference (both views resolve an allowlisted algorithm)") {
    for (algorithmKey <- EncryptionAlgorithmKeys) {
      val conf = referencedAlgorithmConf(algorithmKey, "AES256", Some("SSE-KMS"))
      assert(hadoopEncryptionAlgorithm(conf, "data-bucket") == "SSE-KMS")
      assert(
        S3ConfigGate.unsupportedEncryptionAlgorithmReason(conf, DataBucketUris).isEmpty,
        s"expected $algorithmKey to pass")
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines when the unpropagated view resolves SSE-C " +
      "even though the bucket override makes Hadoop select AES256 (a disagreement only " +
      "declines)") {
    for (algorithmKey <- EncryptionAlgorithmKeys) {
      val conf = referencedAlgorithmConf(algorithmKey, "SSE-C", Some("AES256"))
      assert(hadoopEncryptionAlgorithm(conf, "data-bucket") == "AES256")
      val reason = S3ConfigGate.unsupportedEncryptionAlgorithmReason(conf, DataBucketUris)
      assert(reason.isDefined, s"expected a decline for $algorithmKey")
      assert(reason.get.contains("SSE-C"))
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason never prints an algorithm value that expands to the " +
      "customer key, whether the propagated or the raw view resolves it") {
    val customerKey = "TESTKEY-NEVER-PRINT-123"
    val keyRef = varRef("fs.s3a.encryption.key")
    // (global ref, bucket ref): both views, propagated view only, raw view only.
    val shapes = Seq((keyRef, None), ("AES256", Some(keyRef)), (keyRef, Some("AES256")))
    for (algorithmKey <- EncryptionAlgorithmKeys; (globalRef, bucketRef) <- shapes) {
      val conf = referencedAlgorithmConf(algorithmKey, globalRef, bucketRef)
      conf.set("fs.s3a.encryption.key", customerKey)
      val reason = S3ConfigGate.unsupportedEncryptionAlgorithmReason(conf, DataBucketUris)
      assert(reason.isDefined, s"expected a decline for $algorithmKey, $globalRef, $bucketRef")
      assert(reason.get.contains("not a recognised S3 encryption algorithm"))
      assert(!reason.get.contains(customerKey))
    }
  }

  test("providerClassGateReason never prints a provider value that expands to a credential") {
    val secret = "TESTKEY-NEVER-PRINT-123"
    val providerKeys =
      Seq("fs.s3a.aws.credentials.provider", "fs.s3a.bucket.mybucket.aws.credentials.provider")
    for (key <- providerKeys) {
      val conf = new Configuration(false)
      conf.set("fs.s3a.secret.key", secret)
      conf.set(key, varRef("fs.s3a.secret.key"))
      val reason = S3ConfigGate
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined, s"expected a decline for $key")
      assert(!reason.get.contains(secret))
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines CSE-KMS (client-side encryption): the " +
      "native Parquet reader has no client-side decryption layer, so it would read raw " +
      "ciphertext where Hadoop's own reader, which decrypts client-side via the SDK, succeeds") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "CSE-KMS")
    val reason = S3ConfigGate
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.encryption.algorithm"))
    assert(reason.get.contains("CSE-KMS"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines CSE-CUSTOM (client-side encryption) the " +
      "same way as CSE-KMS") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "CSE-CUSTOM")
    val reason = S3ConfigGate
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("CSE-CUSTOM"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines an unrecognized future algorithm string " +
      "(allowlist semantics: anything not positively confirmed transparent declines, rather " +
      "than a blocklist that would silently admit a new Hadoop encryption method)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SOME-FUTURE-ALGORITHM")
    val reason = S3ConfigGate
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    // Only a value from the fixed list of known algorithm names is ever printed.
    assert(reason.get.contains("not a recognised S3 encryption algorithm"))
    assert(!reason.get.contains("SOME-FUTURE-ALGORITHM"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason passes for AES256, SSE-KMS, DSSE-KMS, and for no " +
      "encryption configured at all (S3 decrypts these server-side algorithms transparently " +
      "on GET/HEAD given read permission alone; only SSE-C requires a client-sent key, and " +
      "only CSE-* requires client-side decryption)") {
    for (algorithm <- Seq("AES256", "SSE-KMS", "DSSE-KMS")) {
      val conf = new Configuration(false)
      conf.set("fs.s3a.encryption.algorithm", algorithm)
      conf.set("fs.s3a.encryption.key", "arn:aws:kms:us-east-1:123456789012:key/abc-123")
      assert(
        S3ConfigGate
          .unsupportedEncryptionAlgorithmReason(
            conf,
            Seq(new URI("s3a://mybucket/part-0.parquet")))
          .isEmpty,
        s"expected $algorithm to be allowlisted")
    }

    val unsetConf = new Configuration(false)
    assert(
      S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(
          unsetConf,
          Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines when the algorithm is stored ONLY in a " +
      "JCEKS keystore as SSE-C, naming the algorithm key and value but never any keystore " +
      "material (buildEncryptionSecrets resolves the algorithm via getPassword, which this " +
      "gate now mirrors instead of the JCEKS-blind plain-conf read that used to under-decline " +
      "this case)") {
    withJceks(Map("fs.s3a.encryption.algorithm" -> "SSE-C")) { conf =>
      val reason = S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.encryption.algorithm"))
      assert(reason.get.contains("SSE-C"))
      assert(reason.get.contains("mybucket"))
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason passes a plaintext-only SSE-C algorithm when " +
      "clear-text-fallback is false and no credential provider is configured (getPassword " +
      "masks the plaintext value, so Hadoop's own buildEncryptionSecrets resolves NO algorithm " +
      "and issues plain GETs with no SSE-C key header -- exactly what native issues)") {
    // Admitting is safe on the shape's own terms: S3 enforces the customer-key-header
    // requirement at the protocol level against every reader, so on a genuinely SSE-C-encrypted
    // object both engines fail loudly and identically (400, no header sent), and on an
    // unencrypted object both read the same bytes. No config state here lets Hadoop decrypt
    // while native reads ciphertext.
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SSE-C")
    conf.set("hadoop.security.credential.clear-text-fallback", "false")
    assert(
      S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "unsupportedEncryptionAlgorithmReason passes when the algorithm is stored ONLY in a JCEKS " +
      "keystore as AES256 (allowlisted even through the keystore-aware resolution path)") {
    withJceks(Map("fs.s3a.encryption.algorithm" -> "AES256")) { conf =>
      assert(S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines without throwing when the keystore backing " +
      "the algorithm is corrupt/unreadable (global arm try/catch containment, same pattern as " +
      "s3ConfigDivergenceReason's corrupt-keystore test)") {
    val corruptFile = File.createTempFile("comet-s3-corrupt-encryption-creds", ".jceks")
    try {
      Files.write(corruptFile.toPath, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
      val conf = new Configuration(false)
      conf.set(
        "hadoop.security.credential.provider.path",
        "jceks://file" + corruptFile.getAbsolutePath)
      // Must not throw: a corrupt/unreadable keystore must decline this bucket, not escape and
      // abort planning for the whole session.
      val reason = S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
    } finally {
      corruptFile.delete()
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines on an S3A-scoped provider path immediately " +
      "when resolving the algorithm, without touching a nonexistent keystore (Arm A proves no " +
      "keystore I/O), even though no algorithm key is set in plain conf") {
    val tempDir = Files.createTempDirectory("comet-s3-encryption-no-keystore")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      val reason = S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.security.credential.provider.path"))
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines a global provider path with any entry that " +
      "is not a local keystore, without opening it") {
    val expected = "Native scan cannot verify the S3 settings for data-bucket " +
      "(hadoop.security.credential.provider.path names a credential keystore that is not on " +
      "the local filesystem, and the settings cannot be verified without reading it)"
    val recorder = classOf[OpenRecordingFileSystem].getName
    // (provider path, whether fs.file.impl names a non-default class)
    val shapes = Seq(
      // A scheme of any name can map to S3AFileSystem; only the class decides for Hadoop.
      ("jceks://s3x@keys/s3.jceks", false),
      ("jceks://s3a@keys-bucket/s3.jceks", false),
      ("jceks://file/tmp/local.jceks,jceks://s3@keys-bucket/s3.jceks", false),
      ("jceks://hdfs@namenode/keys.jceks", false),
      ("jceks://viewfs@cluster/keys.jceks", false),
      ("not a uri", false),
      // Hadoop resolves the nested file scheme through fs.file.impl too.
      ("jceks://file/etc/k.jceks", true),
      // A third-party provider factory would run its own code during planning.
      ("vault://file/k", false))
    val failures = shapes.flatMap { case (providerPath, overrideFileImpl) =>
      OpenRecordingFileSystem.opened.set(false)
      val conf = new Configuration(false)
      conf.set("hadoop.security.credential.provider.path", providerPath)
      val schemes = Seq("s3", "s3a", "s3x", "hdfs", "viewfs") ++
        (if (overrideFileImpl) Seq("file") else Nil)
      schemes.foreach { scheme =>
        conf.set(s"fs.$scheme.impl", recorder)
        conf.set(s"fs.$scheme.impl.disable.cache", "true")
      }
      conf.set("fs.s3a.encryption.algorithm", "SSE-C")
      val reason = S3ConfigGate.unsupportedEncryptionAlgorithmReason(conf, DataBucketUris)
      if (OpenRecordingFileSystem.opened.get()) {
        Some(s"$providerPath was opened")
      } else if (!reason.contains(expected)) {
        Some(s"unexpected reason for $providerPath: $reason")
      } else {
        None
      }
    }
    assert(failures.isEmpty, failures.mkString("\n"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason still evaluates a local keystore or user:/// " +
      "provider path, falling back to the plaintext algorithm") {
    for (providerPath <- Seq("jceks://file/tmp/does-not-exist.jceks", "user:///")) {
      val passing = new Configuration(false)
      passing.set("hadoop.security.credential.provider.path", providerPath)
      passing.set("fs.s3a.encryption.algorithm", "AES256")
      assert(
        S3ConfigGate.unsupportedEncryptionAlgorithmReason(passing, DataBucketUris).isEmpty,
        s"expected $providerPath with AES256 to pass")

      val declining = new Configuration(false)
      declining.set("hadoop.security.credential.provider.path", providerPath)
      declining.set("fs.s3a.encryption.algorithm", "SSE-C")
      val reason =
        S3ConfigGate.unsupportedEncryptionAlgorithmReason(declining, DataBucketUris)
      assert(reason.exists(_.contains("fs.s3a.encryption.algorithm=SSE-C")), s"got $reason")
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason does not fire for non-S3 URIs even when SSE-C is " +
      "configured globally (scheme-scoped, no S3 bucket to derive from a file:// or gs:// " +
      "URI)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SSE-C")
    conf.set("fs.s3a.encryption.key", "c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ==")
    assert(
      S3ConfigGate
        .unsupportedEncryptionAlgorithmReason(
          conf,
          Seq(
            new URI("file:///tmp/table/part-0.parquet"),
            new URI("gs://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "hadoopOnlyEndpointGateReason declines a scheme-less fs.s3a.endpoint with SSL disabled, " +
      "since Hadoop addresses it over http:// and native assumes https://") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.endpoint", "minio:9000")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    val reason = S3ConfigGate.hadoopOnlyEndpointGateReason(
      conf,
      Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.connection.ssl.enabled"))
    assert(reason.get.contains("mybucket"))
  }

  test("hadoopOnlyEndpointGateReason admits a scheme-less endpoint with SSL at its default") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.endpoint", "minio:9000")
    assert(
      S3ConfigGate
        .hadoopOnlyEndpointGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "hadoopOnlyEndpointGateReason admits an endpoint that carries its own scheme even with " +
      "SSL disabled, since Hadoop does not rewrite it") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.endpoint", "http://minio:9000")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    assert(
      S3ConfigGate
        .hadoopOnlyEndpointGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "hadoopOnlyEndpointGateReason declines via a short-form per-bucket " +
      "fs.s3a.bucket.mybucket.connection.ssl.enabled=false") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.endpoint", "minio:9000")
    conf.set("fs.s3a.bucket.mybucket.connection.ssl.enabled", "false")
    val reason = S3ConfigGate.hadoopOnlyEndpointGateReason(
      conf,
      Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("mybucket"))
    assert(
      S3ConfigGate
        .hadoopOnlyEndpointGateReason(conf, Seq(new URI("s3a://otherbucket/part-0.parquet")))
        .isEmpty)
  }

  test("hadoopOnlyEndpointGateReason declines when an assumed-role STS endpoint is set") {
    Seq("fs.s3a.assumed.role.sts.endpoint", "fs.s3a.assumed.role.sts.endpoint.region").foreach {
      key =>
        val conf = new Configuration(false)
        conf.set(key, "sts.eu-west-1.amazonaws.com")
        val reason = S3ConfigGate.hadoopOnlyEndpointGateReason(
          conf,
          Seq(new URI("s3a://mybucket/part-0.parquet")))
        assert(reason.isDefined, key)
        assert(reason.get.contains(key))
        assert(reason.get.contains("mybucket"))
    }
  }

  test(
    "hadoopOnlyEndpointGateReason declines a short-form per-bucket assumed-role STS endpoint " +
      "and admits when nothing endpoint-related is configured") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.assumed.role.sts.endpoint", "sts.eu-west-1.amazonaws.com")
    assert(
      S3ConfigGate
        .hadoopOnlyEndpointGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isDefined)
    assert(
      S3ConfigGate
        .hadoopOnlyEndpointGateReason(
          new Configuration(false),
          Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason declines a bucket configured with a global fs.s3a.proxy.host, naming the " +
      "key and bucket but never any proxy credential") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
    conf.set("fs.s3a.proxy.port", "8080")
    conf.set("fs.s3a.proxy.username", "proxyuser")
    conf.set("fs.s3a.proxy.password", "proxySecretValue")
    val reason =
      S3ConfigGate.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.proxy.host"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("proxyuser"))
    assert(!reason.get.contains("proxySecretValue"))
    assert(!reason.get.contains("proxy.internal.example.com"))
  }

  test(
    "proxyGateReason declines via a short-form per-bucket fs.s3a.proxy.host " +
      "(fs.s3a.bucket.mybucket.proxy.host)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.proxy.host", "proxy.internal.example.com")
    val reason =
      S3ConfigGate.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.proxy.host"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "proxyGateReason passes on a lone long-form per-bucket fs.s3a.proxy.host " +
      "(fs.s3a.bucket.mybucket.fs.s3a.proxy.host): propagateBucketOptions folds it into the " +
      "unread key fs.s3a.fs.s3a.proxy.host, and the host's real consumer is a plain getTrimmed " +
      "on the propagated conf that never checks any long-form alias") {
    // S3AUtils#initProxySupport (hadoop-aws 3.3.4) and AWSClientConfig#createProxyConfiguration
    // (3.4.x) both read the host as conf.getTrimmed("fs.s3a.proxy.host", ""), so a lone long
    // alias never routes Hadoop through a proxy, same fold as the fs.s3a.endpoint control above.
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.proxy.host", "proxy.internal.example.com")
    assert(
      S3ConfigGate
        .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason passes when no fs.s3a.proxy.host is configured anywhere (zero-I/O, no " +
      "provider path set)") {
    val conf = new Configuration(false)
    assert(
      S3ConfigGate
        .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason declines only the bucket whose proxy host is actually configured, " +
      "leaving an unrelated bucket unaffected") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.proxied-bucket.proxy.host", "proxy.internal.example.com")
    val declined =
      S3ConfigGate.proxyGateReason(conf, Seq(new URI("s3a://proxied-bucket/part-0.parquet")))
    assert(declined.isDefined)
    assert(declined.get.contains("proxied-bucket"))

    assert(
      S3ConfigGate
        .proxyGateReason(conf, Seq(new URI("s3a://other-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason does not fire for non-S3 URIs even when fs.s3a.proxy.host is configured " +
      "globally (scheme-scoped, no S3 bucket to derive from a file:// or gs:// URI)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
    assert(
      S3ConfigGate
        .proxyGateReason(
          conf,
          Seq(
            new URI("file:///tmp/table/part-0.parquet"),
            new URI("gs://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason declines a plaintext fs.s3a.proxy.host even when a readable global " +
      "credential store is configured and clear-text-fallback is false: the host's real " +
      "consumer is a plain getTrimmed that consults neither the store nor the fallback flag") {
    // getPassword would hide this plaintext host (no store entry, conf fallback disabled), but
    // S3AUtils#initProxySupport / AWSClientConfig#createProxyConfiguration read it via plain
    // getTrimmed and route Hadoop through the proxy anyway, so the gate must still decline.
    withJceks(Map.empty) { conf =>
      conf.set("hadoop.security.credential.clear-text-fallback", "false")
      conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
      val reason =
        S3ConfigGate.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.proxy.host"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("proxy.internal.example.com"))
    }
  }

  test(
    "proxyGateReason passes when fs.s3a.proxy.host exists only as a global credential-store " +
      "entry: the host's real consumer never calls getPassword, so a store-held host cannot " +
      "put a proxy into effect") {
    // The store entry is real and readable; only lookupPassword-family reads (proxy.username,
    // proxy.password) would find it. The host stays empty under plain getTrimmed, so Hadoop
    // itself never uses a proxy here and declining would be pure over-refusal.
    withJceks(Map("fs.s3a.proxy.host" -> "proxy.internal.example.com")) { conf =>
      assert(
        S3ConfigGate
          .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
          .isEmpty)
    }
  }

  test(
    "proxyGateReason passes on an S3A-scoped provider path when no fs.s3a.proxy.host is set " +
      "in plain conf: no keystore, S3A-scoped or otherwise, can supply the host to its real " +
      "consumer, so provider configuration alone proves nothing about the proxy") {
    // The path points at a nonexistent store on purpose: passing here also proves the gate
    // performs no keystore I/O at all for the host, not even to rule the store out.
    val tempDir = Files.createTempDirectory("comet-s3-proxy-no-keystore")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      assert(
        S3ConfigGate
          .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
          .isEmpty)
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "proxyGateReason still declines a plaintext fs.s3a.proxy.host when an S3A-scoped provider " +
      "path is also configured (plain getTrimmed sees the host regardless of any provider)") {
    val tempDir = Files.createTempDirectory("comet-s3-proxy-scoped-provider")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
      val reason =
        S3ConfigGate.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.proxy.host"))
      assert(reason.get.contains("mybucket"))
    } finally {
      Files.delete(tempDir)
    }
  }

  // ---------------------------------------------------------------------------------------
  // Discovery harness: mechanically bounds which fs.s3a.* keys this comparator must know about.
  // A key cannot compile into the comparator without a consumer tier (AllS3ConfigKeys is
  // derived from S3ConfigKeyConsumers), and the tier expectations below pin the assignments.
  //   (a) AllS3ConfigKeys must cover native's NATIVE_S3A_CONFIG_PROPERTIES
  //       (native/core/src/parquet/objectstore/s3.rs, kept equal to that file's call sites by a
  //       Rust test).
  //   (b) A Rust test in s3.rs checks (a) on every native PR by parsing S3ConfigKeyConsumers
  //       as text. The parse is mirrored here and must equal AllS3ConfigKeys, so a reformat
  //       of that block fails this suite rather than silently breaking the Rust check.
  //   (c) Every credential- or encryption-shaped fs.s3a.* key in Hadoop's Constants class must
  //       be covered by AllS3ConfigKeys or individually documented as exempt.
  // ---------------------------------------------------------------------------------------

  test(
    "discovery harness: AllS3ConfigKeys is a superset of native's checked-in " +
      "NATIVE_S3A_CONFIG_PROPERTIES list (native/core/src/parquet/objectstore/s3.rs)") {
    val rustPath =
      S3ConfigGateSuite.findRepoFile("native/core/src/parquet/objectstore/s3.rs")
    rustPath match {
      case None =>
        cancel(
          "Could not locate native/core/src/parquet/objectstore/s3.rs from this checkout; " +
            "skipping the native-key-list superset guard.")
      case Some(file) =>
        val contents = scala.io.Source.fromFile(file, "UTF-8").mkString
        val marker = "NATIVE_S3A_CONFIG_PROPERTIES: &[&str] = &["
        val start = contents.indexOf(marker)
        assert(
          start >= 0,
          s"Expected ${file.getAbsolutePath} to declare NATIVE_S3A_CONFIG_PROPERTIES -- has " +
            "the constant been renamed or removed?")
        val end = contents.indexOf("];", start)
        assert(end > start, "Expected a `];`-terminated array literal after the marker")
        val arrayBody = contents.substring(start + marker.length, end)
        val nativeProperties =
          "\"([^\"]*)\"".r.findAllMatchIn(arrayBody).map(_.group(1)).toSet
        assert(
          nativeProperties.nonEmpty,
          "Parsed zero property names out of NATIVE_S3A_CONFIG_PROPERTIES -- the parser above " +
            "is likely out of sync with the constant's declaration syntax")

        val nativeKeys = nativeProperties.map(p => s"fs.s3a.$p")
        val comparatorKeys = S3ConfigGate.AllS3ConfigKeys.toSet
        val uncovered = nativeKeys.diff(comparatorKeys)
        assert(
          uncovered.isEmpty,
          "Native reads fs.s3a.* key(s) that S3ConfigGate.AllS3ConfigKeys does not compare, " +
            "so a Hadoop-vs-native divergence on any of them would go undetected: " +
            s"${uncovered.toSeq.sorted.mkString(", ")} -- add the missing key(s) to " +
            "AllS3ConfigKeys")
    }
  }

  test(
    "discovery harness: the text parse of S3ConfigKeyConsumers used by the Rust test in " +
      "s3.rs yields exactly AllS3ConfigKeys") {
    val scalaPath = "spark/src/main/scala/org/apache/comet/objectstore/S3ConfigGate.scala"
    S3ConfigGateSuite.findRepoFile(scalaPath) match {
      case None =>
        cancel(s"Could not locate $scalaPath from this checkout; skipping the parse check.")
      case Some(file) =>
        val contents = scala.io.Source.fromFile(file, "UTF-8").mkString
        // Same marker and extraction rule as gate_compares_every_native_s3a_property.
        val marker = "S3ConfigKeyConsumers: Seq[(String, S3ConfigConsumer)] = Seq("
        val start = contents.indexOf(marker)
        assert(
          start >= 0,
          s"${file.getAbsolutePath} no longer declares `$marker`: update the parser here and " +
            "in s3.rs")
        val end = contents.indexOf(")", start + marker.length)
        assert(end > start, "Expected a `)` closing the S3ConfigKeyConsumers Seq(")
        val block = contents.substring(start + marker.length, end)
        val parsed =
          "\"(fs\\.s3a\\.[^\"]*)\"\\s*->".r.findAllMatchIn(block).map(_.group(1)).toSet
        assert(
          parsed == S3ConfigGate.AllS3ConfigKeys.toSet,
          "The Rust test in s3.rs would parse S3ConfigKeyConsumers as " +
            s"${parsed.toSeq.sorted.mkString(", ")}, not AllS3ConfigKeys: keep each entry a " +
            "single \"fs.s3a.<key>\" -> <Tier> literal pair, or update both parsers")
    }
  }

  test(
    "discovery harness: every compared key carries exactly one consumer-tier assignment, and " +
      "the lookupPassword tier is exactly the credential trio (every other compared key's real " +
      "hadoop-aws 3.3.4 consumer is propagateBucketOptions plus a plain Configuration#get " +
      "family call, verified per key in S3ConfigKeyConsumers' doc)") {
    val keys = S3ConfigGate.S3ConfigKeyConsumers.map(_._1)
    assert(
      keys.distinct == keys,
      "S3ConfigKeyConsumers assigns more than one tier to the same key -- exactly one " +
        "classification per key, declared beside it, is the whole point of the list")
    val passwordTier = S3ConfigGate.S3ConfigKeyConsumers.collect {
      case (key, S3ConfigGate.LookupPasswordConsumer) => key
    }
    assert(
      passwordTier == Seq("fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token"),
      "The lookupPassword tier changed. A key belongs there ONLY when its real hadoop-aws " +
        "consumer is S3AUtils#lookupPassword/#lookupBucketSecret -- verify against the " +
        "decompiled call site before updating this expectation, because the wrong tier is not " +
        "merely over-cautious: an equality comparator reading wider than the real consumer can " +
        "produce a false EQUALITY that admits a diverging scan")
  }

  test(
    "discovery harness: every credential/encryption-shaped fs.s3a.* key Hadoop's Constants " +
      "class declares is either compared by AllS3ConfigKeys or individually documented as " +
      "exempt") {
    val constantsClassName = "org.apache.hadoop.fs.s3a.Constants"
    val constantsClass: Option[Class[_]] =
      try {
        Some(Class.forName(constantsClassName))
      } catch {
        case _: ClassNotFoundException => None
      }
    constantsClass match {
      case None =>
        cancel(
          s"$constantsClassName is not on the test classpath (expected via the " +
            "spark-hadoop-cloud test dependency); skipping the sensitive-key coverage guard.")
      case Some(cls) =>
        val allS3aKeys = cls.getFields
          .filter { f =>
            f.getType == classOf[String] &&
            java.lang.reflect.Modifier.isStatic(f.getModifiers)
          }
          .flatMap { f =>
            f.get(null) match {
              case s: String if s.startsWith("fs.s3a.") => Some(s)
              case _ => None
            }
          }
          .toSet
        assert(
          allS3aKeys.size > 20,
          s"Expected many fs.s3a.* keys via reflection on $constantsClassName, found only " +
            s"${allS3aKeys.size} -- has the class's field layout changed in a way this " +
            "reflection no longer handles?")

        val sensitiveNameFragments =
          Seq("key", "secret", "token", "password", "encryption")
        val sensitiveKeys = allS3aKeys.filter { key =>
          val lower = key.toLowerCase(Locale.ROOT)
          sensitiveNameFragments.exists(lower.contains)
        }

        val comparatorKeys = S3ConfigGate.AllS3ConfigKeys.toSet
        // Individually justified, one at a time -- NOT a blanket "everything encryption-shaped
        // is exempt" carve-out, which would have hidden the SSE-C long-bucket-alias gap just as easily as
        // never checking at all.
        val documentedExempt: Map[String, String] = Map(
          "fs.s3a.encryption.algorithm" ->
            ("handled by the dedicated unsupportedEncryptionAlgorithmReason/" +
              "effectiveEncryptionAlgorithm allowlist gate, not the generic comparator (needs " +
              "its own canonical/deprecated resolution cascade, not a flat single-key compare)"),
          "fs.s3a.server-side-encryption-algorithm" ->
            "deprecated alias of fs.s3a.encryption.algorithm, same dedicated gate",
          "fs.s3a.encryption.key" ->
            ("key MATERIAL for the algorithm above; never read for comparison at all -- the " +
              "allowlist gate declines on the ALGORITHM alone, so the key's value cannot " +
              "change the outcome, and never appears in a decline reason (see " +
              "effectiveEncryptionAlgorithm's doc)"),
          "fs.s3a.server-side-encryption.key" ->
            "deprecated alias of fs.s3a.encryption.key, same reasoning",
          "fs.s3a.encryption.cse.kms.region" ->
            ("CSE tuning, newer Hadoop only: consulted solely when the algorithm resolves to " +
              "a CSE variant, and the allowlist gate declines every CSE algorithm outright, " +
              "so this value can never influence an admitted scan; native never reads it"),
          "fs.s3a.encryption.cse.custom.keyring.class.name" ->
            "CSE tuning, newer Hadoop only, same reasoning as fs.s3a.encryption.cse.kms.region",
          "fs.s3a.encryption.cse.v1.compatibility.enabled" ->
            "CSE tuning, newer Hadoop only, same reasoning as fs.s3a.encryption.cse.kms.region",
          "fs.s3a.proxy.password" ->
            ("covered via the dedicated fs.s3a.proxy.host gate (proxyGateReason/" +
              "unsupportedProxyReason), not the generic comparator: the password (and the " +
              "sibling fs.s3a.proxy.username, not sensitive-shaped so never reaches this map) " +
              "only matters once a proxy is actually in effect, and any bucket with a " +
              "non-empty effective fs.s3a.proxy.host now declines outright, before any " +
              "credential comparison would even run -- so the deployment shape this key used " +
              "to be a KNOWN GAP for (a Hadoop deployment requiring a proxy for S3 egress " +
              "being silently claimed and connected to directly) can no longer reach this key " +
              "at all; the password's VALUE itself is still never read or forwarded to native, " +
              "same as before"),
          "fs.s3a.failinject.inconsistency.key.substring" ->
            ("hadoop-aws test-only S3 fault-injection knob (InconsistentAmazonS3Client " +
              "family), not a credential; matches the sensitive-name heuristic only " +
              "incidentally via \"key.substring\""))

        val unclassified = sensitiveKeys
          .diff(comparatorKeys)
          .diff(documentedExempt.keySet)
        assert(
          unclassified.isEmpty,
          "Hadoop's Constants class declares credential/encryption-shaped fs.s3a.* key(s) " +
            "this discovery harness has never classified (neither compared by " +
            s"AllS3ConfigKeys nor documented as exempt above): ${unclassified.toSeq.sorted
                .mkString(", ")} -- decide whether the key needs a gate, then either add it to " +
            "AllS3ConfigKeys or add a justified entry to `documentedExempt` in this test")
    }
  }

  // Native routes credentials through Comet's credential-provider SPI whenever
  // comet.credential.provider.class resolves non-blank (s3.rs lookup_provider_class) and then
  // never reads fs.s3a.aws.credentials.provider, so the Hadoop-side provider settings must not
  // decline a scan that the documented built-in adapters make work.
  private val SpiAdapter = "org.apache.comet.cloud.s3.HadoopS3ACredentialProviderAdapter"
  private val HadoopOnlyChain = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
  private val SpiUris = Seq(new URI("s3a://spi-bucket/part-0.parquet"))

  test("providerClassGateReason admits a Hadoop-only provider chain when the Comet SPI is set") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.aws.credentials.provider", HadoopOnlyChain)
    assert(S3ConfigGate.providerClassGateReason(conf, SpiUris).isDefined)

    conf.set("fs.s3a.comet.credential.provider.class", SpiAdapter)
    assert(S3ConfigGate.providerClassGateReason(conf, SpiUris).isEmpty)
    assert(S3ConfigGate.declineReason(conf, SpiUris).isEmpty)
  }

  test("providerClassGateReason honors a per-bucket Comet SPI setting and a blank opt-out") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.aws.credentials.provider", HadoopOnlyChain)
    conf.set("fs.s3a.bucket.spi-bucket.comet.credential.provider.class", SpiAdapter)
    assert(S3ConfigGate.providerClassGateReason(conf, SpiUris).isEmpty)
    // The SPI applies to its own bucket only.
    val other = Seq(new URI("s3a://other-bucket/part-0.parquet"))
    assert(S3ConfigGate.providerClassGateReason(conf, other).isDefined)

    // A blank per-bucket value overrides the global one and opts the bucket out of the SPI.
    conf.set("fs.s3a.comet.credential.provider.class", SpiAdapter)
    conf.set("fs.s3a.bucket.spi-bucket.comet.credential.provider.class", " ")
    assert(S3ConfigGate.providerClassGateReason(conf, SpiUris).isDefined)
    assert(S3ConfigGate.providerClassGateReason(conf, other).isEmpty)
  }

  test("assumedRolePolicyGateReason admits a session policy when the Comet SPI is set") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.assumed.role.policy", "{\"Version\":\"2012-10-17\"}")
    assert(S3ConfigGate.assumedRolePolicyGateReason(conf, SpiUris).isDefined)

    conf.set("fs.s3a.comet.credential.provider.class", SpiAdapter)
    assert(S3ConfigGate.assumedRolePolicyGateReason(conf, SpiUris).isEmpty)
  }

  test("the Comet SPI does not relax gates that apply to the store itself") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.comet.credential.provider.class", SpiAdapter)
    conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
    assert(S3ConfigGate.declineReason(conf, SpiUris).exists(_.contains("fs.s3a.proxy.host")))
  }

  // With the SPI active, native never reads its own credential chain keys, and the built-in
  // adapters resolve long-form and per-bucket aliases exactly as Hadoop does, so credentials held
  // only in long-form bucket aliases must not decline the scan.
  private def spiAdapterConf(): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.comet.credential.provider.class", SpiAdapter)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.bucket.spi-bucket.fs.s3a.access.key", "AKIALONGSPI")
    conf.set("fs.s3a.bucket.spi-bucket.fs.s3a.secret.key", "longSpiSecret")
    conf
  }

  test("declineReason admits long-form bucket credentials when the Comet SPI is set") {
    val conf = spiAdapterConf()
    assert(S3ConfigGate.declineReason(conf, SpiUris).isEmpty)

    // Without the SPI the same conf diverges: native's short-then-global read never sees them.
    conf.unset("fs.s3a.comet.credential.provider.class")
    val reason = S3ConfigGate.declineReason(conf, SpiUris)
    assert(reason.exists(_.contains("fs.s3a.access.key")))
    assert(!reason.get.contains("AKIALONGSPI"))
  }

  test(
    "the Comet SPI still gets the credential comparison when Hadoop resolves through a keystore") {
    // The adapter rebuilds its Configuration from the forwarded fs.s3a.* options and cannot read
    // a Hadoop credential provider, so these buckets keep the full comparison.
    val scoped = spiAdapterConf()
    scoped.set("fs.s3a.security.credential.provider.path", "jceks://file/does-not-exist.jceks")
    assert(S3ConfigGate.declineReason(scoped, SpiUris).isDefined)

    val global = spiAdapterConf()
    global.set("hadoop.security.credential.provider.path", "jceks://hdfs/nn/creds.jceks")
    assert(S3ConfigGate.declineReason(global, SpiUris).isDefined)
  }

  test("the Comet SPI still gets the credential comparison when clear-text fallback is off") {
    val conf = spiAdapterConf()
    conf.set("hadoop.security.credential.clear-text-fallback", "false")
    conf.set("fs.s3a.access.key", "AKIAPLAIN")
    conf.set("fs.s3a.secret.key", "plainSecret")
    assert(S3ConfigGate.declineReason(conf, SpiUris).isDefined)
  }

  test("the Comet SPI still compares the store-level keys") {
    val conf = spiAdapterConf()
    conf.set("fs.s3a.custom.ref", "https://store-a.example")
    conf.set("fs.s3a.bucket.spi-bucket.custom.ref", "https://store-b.example")
    conf.set("fs.s3a.bucket.spi-bucket.endpoint", varRef("fs.s3a.custom.ref"))
    conf.set("fs.s3a.bucket.spi-bucket.fs.s3a.endpoint", "https://store-a.example")
    assert(S3ConfigGate.declineReason(conf, SpiUris).exists(_.contains("fs.s3a.endpoint")))
  }

  test("hadoopOnlyEndpointGateReason admits an STS endpoint when the Comet SPI is set") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.assumed.role.sts.endpoint", "sts.internal.example.com")
    assert(S3ConfigGate.hadoopOnlyEndpointGateReason(conf, SpiUris).isDefined)

    conf.set("fs.s3a.comet.credential.provider.class", SpiAdapter)
    assert(S3ConfigGate.hadoopOnlyEndpointGateReason(conf, SpiUris).isEmpty)

    // The scheme-less endpoint check applies to the store itself, so the SPI does not relax it.
    conf.set("fs.s3a.endpoint", "minio.internal.example.com:9000")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    assert(S3ConfigGate.hadoopOnlyEndpointGateReason(conf, SpiUris).isDefined)
  }

  test("the Comet SPI compares credential references a bucket override shadows") {
    // Native forwards each value expanded against the unpropagated conf, so the adapter's own
    // propagateBucketOptions can no longer redirect a reference: it receives the global
    // credentials where Hadoop resolves the bucket's.
    val conf = new Configuration(false)
    conf.set("fs.s3a.comet.credential.provider.class", SpiAdapter)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.access.key", varRef("fs.s3a.custom.access"))
    conf.set("fs.s3a.secret.key", varRef("fs.s3a.custom.secret"))
    conf.set("fs.s3a.custom.access", "AKIAGLOBALREF")
    conf.set("fs.s3a.custom.secret", "globalRefSecret")
    conf.set("fs.s3a.bucket.spi-bucket.custom.access", "AKIABUCKETREF")
    conf.set("fs.s3a.bucket.spi-bucket.custom.secret", "bucketRefSecret")

    // Real Hadoop code: propagation runs before expansion, so Hadoop selects the bucket's values.
    val propagated = S3AUtils.propagateBucketOptions(conf, "spi-bucket")
    assert(propagated.get("fs.s3a.access.key") == "AKIABUCKETREF")

    val reason = S3ConfigGate.declineReason(conf, SpiUris)
    assert(reason.exists(_.contains("fs.s3a.access.key")))
    assert(!reason.get.contains("AKIAGLOBALREF"))
    assert(!reason.get.contains("AKIABUCKETREF"))

    // The same references are fine once the override agrees with the global value.
    conf.set("fs.s3a.bucket.spi-bucket.custom.access", "AKIAGLOBALREF")
    conf.set("fs.s3a.bucket.spi-bucket.custom.secret", "globalRefSecret")
    assert(S3ConfigGate.declineReason(conf, SpiUris).isEmpty)
  }
}

object S3ConfigGateSuite {

  /**
   * Walks up from a candidate root (the `comet.repo.root` system property when set, otherwise
   * `user.dir`) looking for `relativePath`. Handles both a repo-root working directory and a
   * module-root working directory (e.g. `spark`) without hardcoding either.
   */
  private def findRepoFile(relativePath: String): Option[File] = {
    val startDir = Option(System.getProperty("comet.repo.root"))
      .map(new File(_))
      .getOrElse(new File(System.getProperty("user.dir")))
    Iterator
      .iterate(Option(startDir))(_.flatMap(d => Option(d.getParentFile)))
      .takeWhile(_.isDefined)
      .map(_.get)
      .map(new File(_, relativePath))
      .find(_.isFile)
  }
}

/** A filesystem that records any attempt to open it, then refuses. */
class OpenRecordingFileSystem extends RawLocalFileSystem {
  override def initialize(name: URI, conf: Configuration): Unit = {
    OpenRecordingFileSystem.opened.set(true)
    throw new IOException(s"$name must not be opened")
  }
}

object OpenRecordingFileSystem {
  val opened = new AtomicBoolean(false)
}

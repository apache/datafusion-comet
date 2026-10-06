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

import scala.util.{Failure, Success, Try}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider
import org.apache.hadoop.fs.azurebfs.oauth2.{AccessTokenProvider, AzureADToken}
import org.apache.hadoop.fs.azurebfs.services.KeyProvider

import org.apache.comet.objectstore.AbfsAuthResolver._
import org.apache.comet.objectstore.AbfsReflection.Handles

/**
 * Runs the resolver against the hadoop-azure on the test classpath (3.3.4 on the spark-3.x
 * profiles, 3.4.1, 3.4.2 and 3.5.0 on spark-4.0, 4.1 and 4.2). Cases that depend on a feature one
 * line lacks are gated on the capability probes rather than on a version number.
 */
class AbfsAuthResolverSuite extends AnyFunSuite with Matchers {

  private val account = "myacct.dfs.core.windows.net"
  private val container = "data"
  private val uri = new URI(s"abfss://$container@$account/path/file.parquet")

  private val keyProviderException =
    "org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException"
  private val invalidUriException =
    "org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException"
  private val invalidUriAuthorityException =
    "org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriAuthorityException"
  private val tokenAccessProviderException =
    "org.apache.hadoop.fs.azurebfs.contracts.exceptions.TokenAccessProviderException"

  private lazy val handles: Handles = AbfsReflection.handles(getClass.getClassLoader).get

  private def acct(key: String): String = s"$key.$account"

  private def cont(key: String): String = s"$key.$container.$account"

  private def conf(pairs: (String, String)*): Configuration = {
    val hadoopConf = new Configuration()
    pairs.foreach { case (k, v) => hadoopConf.set(k, v) }
    hadoopConf
  }

  private def resolveWith(pairs: (String, String)*): Outcome = resolve(conf(pairs: _*), uri)

  private def assertNoAzureKeys(options: Map[String, String]): Unit = {
    val leaked = options.keys.filter(_.startsWith("fs."))
    assert(leaked.isEmpty, s"fs.* keys forwarded: $leaked")
  }

  // ---- authorityParts ----

  test("authorityParts - raw authority split once at the first '@', port and case kept") {
    authorityParts(new URI("abfss://data@myacct.dfs.core.windows.net:8080/p")) shouldBe
      Right(("data", "myacct.dfs.core.windows.net:8080"))
    authorityParts(new URI("abfss://Data@MyAcct.DFS.core.windows.net/p")) shouldBe
      Right(("Data", "MyAcct.DFS.core.windows.net"))
    authorityParts(new URI("abfss://my%20data@myacct.dfs.core.windows.net/p")) shouldBe
      Right(("my%20data", "myacct.dfs.core.windows.net"))
    authorityParts(new URI("abfss://data@user@myacct.dfs.core.windows.net/p")) shouldBe
      Right(("data", "user@myacct.dfs.core.windows.net"))
  }

  test("authorityParts - no '@', empty container and null authority fail as Hadoop does") {
    val noAt = "abfss://myacct.dfs.core.windows.net/p"
    authorityParts(new URI(noAt)) shouldBe
      Left(Failed(invalidUriAuthorityException, s"$noAt has invalid authority."))

    val emptyContainer = "abfss://@myacct.dfs.core.windows.net/p"
    authorityParts(new URI(emptyContainer)) shouldBe Left(
      Failed(
        invalidUriException,
        s"Invalid URI '$emptyContainer' has a malformed authority, expected container name. " +
          "Authority takes the form abfs://[<container name>@]<account name>"))

    val nullAuthority = "abfss:///p"
    authorityParts(new URI(nullAuthority)) shouldBe
      Left(Failed(invalidUriAuthorityException, s"$nullAuthority has invalid authority."))
  }

  // ---- reflection ----

  test("handles - the constructor choice matches the classpath's AbfsConfiguration") {
    // Hadoop 3.4.2 added `containerConf` together with the constructor that takes the container.
    val hasContainerConf =
      Try(classOf[AbfsConfiguration].getMethod("containerConf", classOf[String])).isSuccess
    handles.hasContainerScopedConfiguration shouldBe hasContainerConf
    info(
      s"container-scoped=${handles.hasContainerScopedConfiguration} " +
        s"fixedSas=${handles.hasFixedSasTokenProvider} " +
        s"clientAssertion=${handles.hasClientAssertionProvider}")
  }

  test("handles - a failed lookup is not cached; the same class yields the same handles") {
    val loader = new FlippingLoader(getClass.getClassLoader)
    AbfsReflection.handles(loader).isFailure shouldBe true
    AbfsReflection.handles(loader).isFailure shouldBe true
    loader.blocked = false
    // The delegating loader hands back the parent's AbfsConfiguration class, so the cache entry
    // is the parent's too.
    AbfsReflection.handles(loader) shouldBe Success(handles)
  }

  // ---- Unconfigured ----

  test("Unconfigured - no auth-bearing key for the account, even with other Azure keys set") {
    resolveWith() shouldBe Unconfigured
    resolveWith("fs.azure.account.hns.enabled" -> "true") shouldBe Unconfigured
    // Another account's key does not configure this one.
    resolveWith("fs.azure.account.key.other.dfs.core.windows.net" -> "c2VjcmV0") shouldBe
      Unconfigured
    toOptions(Unconfigured) shouldBe Map(Markers.RESOLUTION -> Markers.NONE)
  }

  test("not Unconfigured - OAuth keys without an auth type fail in Hadoop's SharedKey words") {
    // Hadoop defaults to SharedKey, whose key provider finds no account key. The environment must
    // not step in for a misconfigured account.
    val outcome = resolveWith(acct(Keys.CLIENT_ID) -> "client-123")
    outcome shouldBe a[Failed]
    outcome.asInstanceOf[Failed].exceptionClass shouldBe keyProviderException
    assertNoAzureKeys(toOptions(outcome))
  }

  test("not Unconfigured - a fixed SAS token without an auth type, on every hadoop-azure") {
    val outcome = resolveWith(acct(Keys.SAS_FIXED_TOKEN) -> "sv=2020-08-04&sig=fixed-signature")
    outcome shouldBe a[Failed]
    outcome.asInstanceOf[Failed].exceptionClass shouldBe keyProviderException
    assertNoAzureKeys(toOptions(outcome))
  }

  test("not Unconfigured - every auth-bearing key set alone hands the decision to Hadoop") {
    val plainValues = Map(
      Keys.AUTH_TYPE -> "SharedKey",
      Keys.KEY_PROVIDER -> "org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider",
      Keys.SHELL_KEY_PROVIDER_SCRIPT -> "/bin/echo",
      Keys.OAUTH_PROVIDER_TYPE -> Providers.CLIENT_CREDS,
      Keys.SAS_TOKEN_PROVIDER_TYPE -> classOf[NeverInstantiatedSasTokenProvider].getName)
    plainValues.keySet shouldBe plainAuthKeys.toSet
    val cases = plainValues.toSeq ++ passwordAuthKeys.map(_ -> "some-value")
    cases.foreach { case (key, value) =>
      withClue(s"$key alone: ") {
        val outcome = resolveWith(acct(key) -> value)
        outcome should not be Unconfigured
        outcome shouldBe a[Failed]
        assertNoAzureKeys(toOptions(outcome))
      }
    }
  }

  // ---- Declined ----

  test("Declined - Custom auth type is forwarded by name, never instantiated") {
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "Custom",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> classOf[NeverInstantiatedAccessTokenProvider].getName)
    outcome shouldBe Declined("Custom", None)
    toOptions(outcome) shouldBe
      Map(Markers.RESOLUTION -> Markers.DECLINED, Markers.AUTH_TYPE -> "Custom")
  }

  test("Declined - a custom SAS token provider class is named, never constructed") {
    val providerClass = classOf[NeverInstantiatedSasTokenProvider].getName
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "SAS",
      acct(Keys.SAS_TOKEN_PROVIDER_TYPE) -> providerClass,
      acct(Keys.SAS_FIXED_TOKEN) -> "sv=2020-08-04&sig=ignored-when-a-class-is-set")
    outcome shouldBe Declined("SAS", Some(providerClass))
    val options = toOptions(outcome)
    options(Markers.SAS_PROVIDER_CLASS) shouldBe providerClass
    assertNoAzureKeys(options)
  }

  test("Failed - an AccessTokenProvider that is not one of the five built-ins fails in Hadoop") {
    // Hadoop matches the class by `==` and rejects anything else without constructing it.
    val providerClass = classOf[NeverInstantiatedAccessTokenProvider].getName
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> providerClass,
      acct(Keys.CLIENT_ID) -> "client-123")
    outcome shouldBe Failed(
      classOf[IllegalArgumentException].getName,
      s"java.lang.IllegalArgumentException: Failed to initialize class $providerClass")
    assertNoAzureKeys(toOptions(outcome))
  }

  test("Declined - Workload Identity with a client assertion provider (hadoop-azure 3.5.0+)") {
    assume(handles.hasClientAssertionProvider, "no ClientAssertionProvider on this classpath")
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.WORKLOAD_IDENTITY,
      acct(Keys.MSI_TENANT) -> "tenant-abc",
      acct(Keys.CLIENT_ID) -> "client-123",
      acct(Keys.CLIENT_ASSERTION_PROVIDER_TYPE) -> " com.example.NeverLoadedAssertionProvider ")
    outcome shouldBe Declined(
      "OAuth",
      Some(Providers.WORKLOAD_IDENTITY),
      Some("com.example.NeverLoadedAssertionProvider"))
    val options = toOptions(outcome)
    options(Markers.OAUTH_PROVIDER_CLASS) shouldBe Providers.WORKLOAD_IDENTITY
    options(Markers.OAUTH_ASSERTION_PROVIDER_CLASS) shouldBe
      "com.example.NeverLoadedAssertionProvider"
    assertNoAzureKeys(options)
  }

  test("client assertion key is ignored where Hadoop ignores it (3.4.1 and 3.4.2)") {
    assume(
      handles.hasFixedSasTokenProvider && !handles.hasClientAssertionProvider,
      "only on a hadoop-azure with Workload Identity but without ClientAssertionProvider")
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.WORKLOAD_IDENTITY,
      acct(Keys.MSI_TENANT) -> "tenant-abc",
      acct(Keys.CLIENT_ID) -> "client-123",
      acct(Keys.CLIENT_ASSERTION_PROVIDER_TYPE) -> "com.example.NeverLoadedAssertionProvider")
    outcome shouldBe a[Resolved]
    outcome.asInstanceOf[Resolved].providerClass shouldBe Some(Providers.WORKLOAD_IDENTITY)
  }

  // ---- Failed ----

  test("Failed - a bad Base64 account key carries Hadoop's class and a redacted message") {
    val rawKey = "this-is-not-base64!!"
    val outcome = resolveWith(acct(Keys.ACCOUNT_KEY) -> rawKey)
    outcome shouldBe a[Failed]
    val failed = outcome.asInstanceOf[Failed]
    failed.exceptionClass shouldBe keyProviderException
    failed.message should include("KeyProviderException")
    failed.message should include("driver log")
    failed.message should not include rawKey
    val options = toOptions(outcome)
    options(Markers.RESOLUTION) shouldBe Markers.ERROR
    options(Markers.ERROR_CLASS) shouldBe keyProviderException
    options.values.foreach(v => v should not include rawKey)
    assertNoAzureKeys(options)
  }

  test("Failed - a KeyProvider's own IllegalArgumentException is reported by class only") {
    // Hadoop's IllegalArgumentExceptions name keys and classes; a user KeyProvider's may quote a
    // secret the resolver never read, so only its origin decides whether the text is forwarded.
    val outcome =
      resolveWith(acct(Keys.KEY_PROVIDER) -> classOf[SecretLeakingKeyProvider].getName)
    outcome shouldBe Failed(
      classOf[IllegalArgumentException].getName,
      "java.lang.IllegalArgumentException; the full message is in the Spark driver log")
    val options = toOptions(outcome)
    options(Markers.ERROR_CLASS) shouldBe classOf[IllegalArgumentException].getName
    options.values.foreach(v => v should not include SecretLeakingKeyProvider.marker)
    assertNoAzureKeys(options)
  }

  test("Failed - an unknown auth type value is Hadoop's enum error") {
    val outcome = resolveWith(acct(Keys.AUTH_TYPE) -> "oauth")
    outcome shouldBe a[Failed]
    val failed = outcome.asInstanceOf[Failed]
    failed.exceptionClass shouldBe classOf[IllegalArgumentException].getName
    failed.message should include("No enum constant")
    failed.message should include("oauth")
  }

  test("Failed - account-specific OAuth with only a global provider class (auth-type guard)") {
    // getTokenProviderClass reads the global provider class only when the GLOBAL auth type
    // matches, so Hadoop sees no class and fails to initialize the provider.
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      Keys.OAUTH_PROVIDER_TYPE -> Providers.CLIENT_CREDS,
      acct(Keys.CLIENT_ENDPOINT) -> "https://login.microsoftonline.com/tenant/oauth2/token",
      acct(Keys.CLIENT_ID) -> "client-123",
      acct(Keys.CLIENT_SECRET) -> "client-secret-value")
    outcome shouldBe Failed(
      classOf[IllegalArgumentException].getName,
      "java.lang.IllegalArgumentException: Failed to initialize null")
  }

  test("Failed - a missing mandatory OAuth key surfaces Hadoop's cause chain") {
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.CLIENT_CREDS,
      acct(Keys.CLIENT_ENDPOINT) -> "https://login.microsoftonline.com/tenant/oauth2/token",
      acct(Keys.CLIENT_ID) -> "client-123")
    outcome shouldBe a[Failed]
    val failed = outcome.asInstanceOf[Failed]
    failed.exceptionClass shouldBe tokenAccessProviderException
    failed.message should include(s"Configuration property ${Keys.CLIENT_SECRET} not found.")
    assertNoAzureKeys(toOptions(outcome))
  }

  test("Failed - SharedKey with an account name that is not fully qualified") {
    val shortUri = new URI("abfss://data@myacct/path")
    val outcome = resolve(conf("fs.azure.account.key.myacct" -> "c2VjcmV0"), shortUri)
    outcome shouldBe
      Failed(invalidUriException, s"Invalid URI $shortUri - account name is not fully qualified.")
  }

  test("Failed - a LinkageError from the reflection layer is an error marker, not a crash") {
    val outcome = resolve(conf(acct(Keys.ACCOUNT_KEY) -> "c2VjcmV0"), uri, Success(LinkageStub))
    outcome shouldBe a[Failed]
    outcome.asInstanceOf[Failed].exceptionClass shouldBe classOf[NoSuchMethodError].getName
    assertNoAzureKeys(toOptions(outcome))
  }

  test("Failed - hadoop-azure missing from the classpath") {
    val missing = new ClassNotFoundException(AbfsReflection.ClassNames.ABFS_CONFIGURATION)
    val outcome = resolve(conf(acct(Keys.ACCOUNT_KEY) -> "c2VjcmV0"), uri, Failure(missing))
    outcome shouldBe a[Failed]
    val failed = outcome.asInstanceOf[Failed]
    failed.exceptionClass shouldBe classOf[ClassNotFoundException].getName
    failed.message should include("hadoop-azure is not on the driver classpath")
    failed.message should include(AbfsReflection.ClassNames.ABFS_CONFIGURATION)
  }

  test("redact - hides quoted keys and long secrets, leaves short values alone") {
    val message = "Failure for acct key =\"c2VjcmV0c2VjcmV0\": token=the-secret-value port=8080"
    redact(message, Seq("the-secret-value", "8080")) shouldBe
      "Failure for acct key =\"<redacted>\": token=<redacted> port=8080"
  }

  test("redact - a padded secret is hidden in its trimmed form too") {
    val padded = "  the-padded-secret  "
    redact("provider said the-padded-secret was rejected", Seq(padded, padded.trim)) shouldBe
      "provider said <redacted> was rejected"
  }

  // ---- Resolved ----

  test("Resolved - SharedKey from the account-scoped key, forwarded under the global name") {
    val outcome = resolveWith(acct(Keys.ACCOUNT_KEY) -> "c2VjcmV0")
    outcome shouldBe Resolved("SharedKey", None, Map(Keys.ACCOUNT_KEY -> "c2VjcmV0"))
    toOptions(outcome) shouldBe Map(
      Markers.RESOLUTION -> Markers.RESOLVED,
      Markers.AUTH_TYPE -> "SharedKey",
      Keys.ACCOUNT_KEY -> "c2VjcmV0")
  }

  test("Resolved - SharedKey from the global key, with a port-scoped account") {
    resolveWith(Keys.ACCOUNT_KEY -> "Z2xvYmFs") shouldBe
      Resolved("SharedKey", None, Map(Keys.ACCOUNT_KEY -> "Z2xvYmFs"))

    val portUri = new URI("abfss://data@myacct.dfs.core.windows.net:8080/path")
    val portConf = conf(
      "fs.azure.account.key.myacct.dfs.core.windows.net:8080" -> "cG9ydA==",
      acct(Keys.ACCOUNT_KEY) -> "bm9wb3J0")
    resolve(portConf, portUri) shouldBe
      Resolved("SharedKey", None, Map(Keys.ACCOUNT_KEY -> "cG9ydA=="))
  }

  test("Resolved - mixed-case account names are matched exactly, as Hadoop does") {
    val mixedUri = new URI("abfss://data@MyAcct.dfs.core.windows.net/path")
    resolve(
      conf("fs.azure.account.key.MyAcct.dfs.core.windows.net" -> "bWl4ZWQ="),
      mixedUri) shouldBe
      Resolved("SharedKey", None, Map(Keys.ACCOUNT_KEY -> "bWl4ZWQ="))
    // The lower-case key belongs to a different account name.
    resolve(
      conf("fs.azure.account.key.myacct.dfs.core.windows.net" -> "bWl4ZWQ="),
      mixedUri) shouldBe
      Unconfigured
  }

  test("Resolved - ClientCreds forwards only its keys, never another account's") {
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.CLIENT_CREDS,
      acct(Keys.CLIENT_ENDPOINT) -> "https://login.microsoftonline.com/tenant/oauth2/token",
      acct(Keys.CLIENT_ID) -> "client-123",
      acct(Keys.CLIENT_SECRET) -> "client-secret-value",
      // Keys MSI would read, and another account's secret: none of them may travel.
      acct(Keys.MSI_TENANT) -> "tenant-abc",
      "fs.azure.account.oauth2.client.secret.other.dfs.core.windows.net" -> "other-secret-value")
    outcome shouldBe Resolved(
      "OAuth",
      Some(Providers.CLIENT_CREDS),
      Map(
        Keys.CLIENT_ENDPOINT -> "https://login.microsoftonline.com/tenant/oauth2/token",
        Keys.CLIENT_ID -> "client-123",
        Keys.CLIENT_SECRET -> "client-secret-value"))
    val options = toOptions(outcome)
    options(Markers.OAUTH_PROVIDER_CLASS) shouldBe Providers.CLIENT_CREDS
    options.values.foreach(v => v should not include "other-secret-value")
    options.keys.filter(_.startsWith("fs.")) should contain theSameElementsAs
      Seq(Keys.CLIENT_ENDPOINT, Keys.CLIENT_ID, Keys.CLIENT_SECRET)
  }

  test("Resolved - global OAuth configuration applies when the global auth type matches") {
    val globalConf = conf(
      Keys.AUTH_TYPE -> "OAuth",
      Keys.OAUTH_PROVIDER_TYPE -> Providers.CLIENT_CREDS,
      Keys.CLIENT_ENDPOINT -> "https://login.microsoftonline.com/tenant/oauth2/token",
      Keys.CLIENT_ID -> "global-client",
      // A variable reference resolves through Configuration#get, as it does for Hadoop. Built by
      // concatenation so scalac does not read it as a string missing its interpolator.
      "my.client.secret" -> "substituted-secret",
      Keys.CLIENT_SECRET -> ("${" + "my.client.secret}"))
    resolve(globalConf, uri) shouldBe Resolved(
      "OAuth",
      Some(Providers.CLIENT_CREDS),
      Map(
        Keys.CLIENT_ENDPOINT -> "https://login.microsoftonline.com/tenant/oauth2/token",
        Keys.CLIENT_ID -> "global-client",
        Keys.CLIENT_SECRET -> "substituted-secret"))
  }

  test("Resolved - MSI applies Hadoop's defaults, trimming and the authority slash") {
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.MSI,
      acct(Keys.MSI_TENANT) -> "tenant-abc",
      acct(Keys.CLIENT_ID) -> "client-123",
      acct(Keys.MSI_AUTHORITY) -> " https://login.example.net ")
    outcome shouldBe Resolved(
      "OAuth",
      Some(Providers.MSI),
      Map(
        Keys.MSI_ENDPOINT -> "http://169.254.169.254/metadata/identity/oauth2/token",
        Keys.MSI_TENANT -> "tenant-abc",
        Keys.CLIENT_ID -> "client-123",
        Keys.MSI_AUTHORITY -> "https://login.example.net/"))
  }

  test("Resolved - MSI without tenant and client id (optional from hadoop-azure 3.4.1)") {
    assume(handles.hasFixedSasTokenProvider, "hadoop-azure 3.3.4 makes both keys mandatory")
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.MSI,
      acct(Keys.MSI_AUTHORITY) -> "")
    outcome shouldBe Resolved(
      "OAuth",
      Some(Providers.MSI),
      Map(
        Keys.MSI_ENDPOINT -> "http://169.254.169.254/metadata/identity/oauth2/token",
        Keys.MSI_AUTHORITY -> "https://login.microsoftonline.com/"))
  }

  test("Resolved - Workload Identity with the default token file (hadoop-azure 3.4.1+)") {
    assume(handles.hasFixedSasTokenProvider, "no WorkloadIdentityTokenProvider on 3.3.4")
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.WORKLOAD_IDENTITY,
      acct(Keys.MSI_TENANT) -> "tenant-abc",
      acct(Keys.CLIENT_ID) -> "client-123")
    outcome shouldBe Resolved(
      "OAuth",
      Some(Providers.WORKLOAD_IDENTITY),
      Map(
        Keys.MSI_AUTHORITY -> "https://login.microsoftonline.com/",
        Keys.MSI_TENANT -> "tenant-abc",
        Keys.CLIENT_ID -> "client-123",
        Keys.TOKEN_FILE -> "/var/run/secrets/azure/tokens/azure-identity-token"))
  }

  test("Resolved - container-scoped keys win on 3.4.2+ and are ignored before") {
    assume(handles.hasFixedSasTokenProvider, "no WorkloadIdentityTokenProvider on 3.3.4")
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "OAuth",
      acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.WORKLOAD_IDENTITY,
      acct(Keys.MSI_TENANT) -> "tenant-abc",
      acct(Keys.CLIENT_ID) -> "account-client",
      cont(Keys.CLIENT_ID) -> "container-client",
      cont(Keys.TOKEN_FILE) -> " /mnt/tokens/container-token ")
    val expectedClient =
      if (handles.hasContainerScopedConfiguration) "container-client" else "account-client"
    val expectedTokenFile =
      if (handles.hasContainerScopedConfiguration) "/mnt/tokens/container-token"
      else "/var/run/secrets/azure/tokens/azure-identity-token"
    outcome shouldBe Resolved(
      "OAuth",
      Some(Providers.WORKLOAD_IDENTITY),
      Map(
        Keys.MSI_AUTHORITY -> "https://login.microsoftonline.com/",
        Keys.MSI_TENANT -> "tenant-abc",
        Keys.CLIENT_ID -> expectedClient,
        Keys.TOKEN_FILE -> expectedTokenFile))
  }

  test("Resolved - fixed SAS token is trimmed and forwarded (hadoop-azure 3.4.1+)") {
    assume(handles.hasFixedSasTokenProvider, "no fixed SAS token on 3.3.4")
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "SAS",
      acct(Keys.SAS_FIXED_TOKEN) -> " ?sv=2020-08-04&sig=fixed-signature ")
    outcome shouldBe
      Resolved("SAS", None, Map(Keys.SAS_FIXED_TOKEN -> "?sv=2020-08-04&sig=fixed-signature"))
    toOptions(outcome) should not contain key(Markers.SAS_PROVIDER_CLASS)
  }

  test("Failed - SAS without a provider class on hadoop-azure 3.3.4, which has no fixed token") {
    assume(!handles.hasFixedSasTokenProvider, "3.4.1+ accept the fixed token")
    val outcome = resolveWith(
      acct(Keys.AUTH_TYPE) -> "SAS",
      acct(Keys.SAS_FIXED_TOKEN) -> "sv=2020-08-04&sig=fixed-signature")
    outcome shouldBe a[Failed]
    outcome.asInstanceOf[Failed].exceptionClass shouldBe tokenAccessProviderException
    assertNoAzureKeys(toOptions(outcome))
  }

  test("Failed - SAS with neither class nor token (hadoop-azure 3.4.1+)") {
    assume(handles.hasFixedSasTokenProvider, "3.3.4 fails differently, covered above")
    val outcome = resolveWith(acct(Keys.AUTH_TYPE) -> "SAS", acct(Keys.SAS_FIXED_TOKEN) -> "  ")
    outcome shouldBe a[Failed]
    outcome.asInstanceOf[Failed].exceptionClass shouldBe
      "org.apache.hadoop.fs.azurebfs.contracts.exceptions.SASTokenProviderException"
  }
}

/** A delegating loader that refuses `AbfsConfiguration` until `blocked` is cleared. */
class FlippingLoader(parent: ClassLoader) extends ClassLoader(parent) {
  @volatile var blocked = true

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    if (blocked && name == AbfsReflection.ClassNames.ABFS_CONFIGURATION) {
      throw new ClassNotFoundException(name)
    }
    super.loadClass(name, resolve)
  }
}

object SecretLeakingKeyProvider {
  val marker = "vault-secret-marker-7b3e9c"
}

/** A KeyProvider whose failure quotes a secret the resolver never read from the conf. */
class SecretLeakingKeyProvider extends KeyProvider {
  override def getStorageAccountKey(accountName: String, conf: Configuration): String =
    throw new IllegalArgumentException(s"bad key ${SecretLeakingKeyProvider.marker}")
}

/** Fails loudly if anything constructs it: the resolver must only ever name this class. */
class NeverInstantiatedSasTokenProvider extends SASTokenProvider {
  NeverInstantiated.fail("NeverInstantiatedSasTokenProvider")

  override def initialize(configuration: Configuration, accountName: String): Unit = ()

  override def getSASToken(
      account: String,
      fileSystem: String,
      path: String,
      operation: String): String =
    throw new UnsupportedOperationException("not reached")
}

/** An `AccessTokenProvider` that is not one of Hadoop's built-ins; never constructed. */
class NeverInstantiatedAccessTokenProvider extends AccessTokenProvider {
  NeverInstantiated.fail("NeverInstantiatedAccessTokenProvider")

  override protected def refreshToken(): AzureADToken =
    throw new UnsupportedOperationException("not reached")
}

/** Handles whose first call fails with a `LinkageError`, as a mismatched hadoop-azure would. */
object LinkageStub extends Handles {
  // Built here so the file has no `throw new ...Error(` (scalastyle); thrown below.
  private val linkageFailure = new NoSuchMethodError("stub: AbfsConfiguration.<init>")

  private def notReached: Nothing = throw new UnsupportedOperationException("not reached")

  override def hasContainerScopedConfiguration: Boolean = false
  override def hasFixedSasTokenProvider: Boolean = false
  override def hasClientAssertionProvider: Boolean = false
  override def accessTokenProviderClass: Class[_] = classOf[AnyRef]
  override def sasTokenProviderClass: Class[_] = classOf[AnyRef]
  override def newAbfsConfiguration(
      conf: Configuration,
      account: String,
      container: String,
      uri: URI): AnyRef = throw linkageFailure
  override def newKeyProviderConfiguration(conf: Configuration, account: String): AnyRef =
    notReached
  override def getAuthType(abfsConf: AnyRef, account: String): Enum[_] = notReached
  override def get(abfsConf: AnyRef, key: String): Option[String] = notReached
  override def getPasswordString(abfsConf: AnyRef, key: String): Option[String] = notReached
  override def getTokenProviderClass(
      abfsConf: AnyRef,
      authType: Enum[_],
      key: String,
      xface: Class[_]): Option[Class[_]] = notReached
  override def getStorageAccountKey(abfsConf: AnyRef): String = notReached
  override def getTokenProvider(abfsConf: AnyRef): Unit = notReached
  override def getSASTokenProvider(abfsConf: AnyRef): Unit = notReached
  override def authDefault(fieldName: String): String = notReached
}

/** Throws from a constructor without leaving the compiler a dead-code warning to report. */
object NeverInstantiated {
  def fail(name: String): Unit = throw new IllegalStateException(s"$name was constructed")
}

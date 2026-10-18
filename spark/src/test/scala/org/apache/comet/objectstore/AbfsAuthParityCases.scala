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
import java.util.{Date, Locale}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.azurebfs.extensions.{CustomTokenProviderAdaptee, SASTokenProvider}
import org.apache.hadoop.fs.azurebfs.services.KeyProvider

import org.apache.comet.objectstore.AbfsAuthResolver.{Keys, Providers}

/**
 * The configurations [[AbfsAuthParitySuite]] runs through the resolver and through the real
 * `AzureBlobFileSystem`. Built in code so nothing checked in can drift from the cases; the
 * recorded expectation per hadoop-azure version lives in `abfs-auth-parity/expected-*.json`.
 *
 * A case may carry `pins`: the outcome kind and error class that reading the hadoop-azure source
 * predicts per version. Every `calibration` case has one for all four versions, so the recorded
 * files are checked against that reading rather than trusted.
 */
object AbfsAuthParityCases {

  val VERSIONS: Seq[String] = Seq("3.3.4", "3.4.1", "3.4.2", "3.5.0")

  object Outcomes {
    val RESOLVED = "resolved"
    val UNCONFIGURED = "unconfigured"
    val DECLINED = "declined"
    val FAILED = "failed"
  }

  object ExceptionClasses {
    private val pkg = "org.apache.hadoop.fs.azurebfs.contracts.exceptions."
    val KEY_PROVIDER: String = pkg + "KeyProviderException"
    val INVALID_URI: String = pkg + "InvalidUriException"
    val INVALID_URI_AUTHORITY: String = pkg + "InvalidUriAuthorityException"
    val TOKEN_ACCESS_PROVIDER: String = pkg + "TokenAccessProviderException"
    val SAS_TOKEN_PROVIDER: String = pkg + "SASTokenProviderException"
    val CONFIGURATION_PROPERTY_NOT_FOUND: String = pkg + "ConfigurationPropertyNotFoundException"
    val ILLEGAL_ARGUMENT: String = classOf[IllegalArgumentException].getName
    val RUNTIME: String = classOf[RuntimeException].getName
    val ARRAY_INDEX: String = classOf[ArrayIndexOutOfBoundsException].getName
  }

  /** The outcome the hadoop-azure source predicts for one version. */
  final case class Pin(outcome: String, errorClass: Option[String] = None)

  object Pin {
    val resolved: Pin = Pin(Outcomes.RESOLVED)
    val unconfigured: Pin = Pin(Outcomes.UNCONFIGURED)
    val declined: Pin = Pin(Outcomes.DECLINED)
    def failed(exceptionClass: String): Pin = Pin(Outcomes.FAILED, Some(exceptionClass))
  }

  /**
   * @param liveValues
   *   forwarded values that depend on the machine (an environment variable); the suite checks
   *   them directly and records a placeholder instead.
   */
  final case class ParityCase(
      id: String,
      group: String,
      uri: URI,
      conf: Map[String, String],
      note: String,
      pins: Map[String, Pin] = Map.empty,
      liveValues: Map[String, String] = Map.empty)

  def pins(v334: Pin, v341: Pin, v342: Pin, v350: Pin): Map[String, Pin] =
    Map("3.3.4" -> v334, "3.4.1" -> v341, "3.4.2" -> v342, "3.5.0" -> v350)

  def allVersions(pin: Pin): Map[String, Pin] = pins(pin, pin, pin, pin)

  /** `before` on 3.3.4, `after` from 3.4.1 (fixed SAS token, Workload Identity, optional MSI). */
  def from341(before: Pin, after: Pin): Map[String, Pin] = pins(before, after, after, after)

  /** `before` up to 3.4.1, `after` from 3.4.2 (container-scoped keys). */
  def from342(before: Pin, after: Pin): Map[String, Pin] = pins(before, before, after, after)

  /** `before` up to 3.4.2, `after` on 3.5.0 (user-bound SAS, client assertion provider). */
  def from350(before: Pin, after: Pin): Map[String, Pin] = pins(before, before, before, after)

  val ACCOUNT = "myacct.dfs.core.windows.net"
  val CONTAINER = "data"
  val URI_DFS = new URI(s"abfss://$CONTAINER@$ACCOUNT/path/file.parquet")

  // Base64 of "secret", "global", "container", "null", "custom", "port", "env".
  val KEY_ACCOUNT = "c2VjcmV0"
  val KEY_GLOBAL = "Z2xvYmFs"
  val KEY_CONTAINER = "Y29udGFpbmVy"
  val KEY_NULL_LITERAL = "bnVsbA=="
  val KEY_CUSTOM = "Y3VzdG9t"
  val KEY_PORT = "cG9ydA=="
  val KEY_ENV = "ZW52"

  val ENDPOINT = "https://login.microsoftonline.com/tenant-abc/oauth2/token"
  val CLIENT_ID = "client-123"
  val CLIENT_SECRET = "client-secret-value"
  val TENANT = "tenant-abc"
  val FIXED_SAS = "sv=2020-08-04&sig=fixed-signature"
  val DEFAULT_MSI_ENDPOINT = "http://169.254.169.254/metadata/identity/oauth2/token"
  val DEFAULT_AUTHORITY = "https://login.microsoftonline.com/"
  val DEFAULT_TOKEN_FILE = "/var/run/secrets/azure/tokens/azure-identity-token"

  val SIMPLE_KEY_PROVIDER = "org.apache.hadoop.fs.azurebfs.services.SimpleKeyProvider"
  val SHELL_KEY_PROVIDER = "org.apache.hadoop.fs.azurebfs.services.ShellDecryptionKeyProvider"
  val MISSING_CLASS = "com.example.comet.MissingProvider"
  val NOT_A_PROVIDER: String = classOf[String].getName
  val CUSTOM_ADAPTEE: String = classOf[ParityCustomTokenAdaptee].getName
  val SAS_PROVIDER: String = classOf[ParitySasTokenProvider].getName
  val FOREIGN_ACCESS_TOKEN_PROVIDER: String =
    classOf[NeverInstantiatedAccessTokenProvider].getName

  /** The aliases the suite stores in its JCEKS keystore, with no clear-text counterpart. */
  val credentialEntries: Map[String, String] =
    Map(acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT, acct(Keys.CLIENT_SECRET) -> CLIENT_SECRET)

  /**
   * The variable `${env.COMET_ABFS_PARITY_UNSET}` falls back to; the suite asserts it is unset.
   */
  val UNSET_ENV_VAR = "COMET_ABFS_PARITY_UNSET"

  def acct(key: String, account: String = ACCOUNT): String = s"$key.$account"

  def cont(key: String, container: String = CONTAINER, account: String = ACCOUNT): String =
    s"$key.$container.$account"

  private def uriFor(container: String, account: String): URI =
    new URI(s"abfss://$container@$account/path/file.parquet")

  private val clientCreds: Map[String, String] = Map(
    acct(Keys.AUTH_TYPE) -> "OAuth",
    acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.CLIENT_CREDS,
    acct(Keys.CLIENT_ENDPOINT) -> ENDPOINT,
    acct(Keys.CLIENT_ID) -> CLIENT_ID,
    acct(Keys.CLIENT_SECRET) -> CLIENT_SECRET)

  private val msi: Map[String, String] = Map(
    acct(Keys.AUTH_TYPE) -> "OAuth",
    acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.MSI,
    acct(Keys.MSI_TENANT) -> TENANT,
    acct(Keys.CLIENT_ID) -> CLIENT_ID)

  private val workloadIdentity: Map[String, String] = Map(
    acct(Keys.AUTH_TYPE) -> "OAuth",
    acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.WORKLOAD_IDENTITY,
    acct(Keys.MSI_TENANT) -> TENANT,
    acct(Keys.CLIENT_ID) -> CLIENT_ID)

  private val refreshToken: Map[String, String] = Map(
    acct(Keys.AUTH_TYPE) -> "OAuth",
    acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.REFRESH_TOKEN,
    acct(Keys.REFRESH_TOKEN) -> "refresh-token-value",
    acct(Keys.CLIENT_ID) -> CLIENT_ID)

  private val userPassword: Map[String, String] = Map(
    acct(Keys.AUTH_TYPE) -> "OAuth",
    acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.USER_PASSWORD,
    acct(Keys.CLIENT_ENDPOINT) -> ENDPOINT,
    acct(Keys.USER_NAME) -> "user@example.com",
    acct(Keys.USER_PASSWORD) -> "user-password-value")

  private val sasCustom: Map[String, String] =
    Map(acct(Keys.AUTH_TYPE) -> "SAS", acct(Keys.SAS_TOKEN_PROVIDER_TYPE) -> SAS_PROVIDER)

  // 3.3.4 has no fixed SAS token, so a configuration that names no provider class fails there in
  // TokenAccessProviderException and in SASTokenProviderException from 3.4.1.
  private val fixedSasOnly: Map[String, Pin] =
    from341(Pin.failed(ExceptionClasses.TOKEN_ACCESS_PROVIDER), Pin.resolved)
  private val noSasSource: Map[String, Pin] = from341(
    Pin.failed(ExceptionClasses.TOKEN_ACCESS_PROVIDER),
    Pin.failed(ExceptionClasses.SAS_TOKEN_PROVIDER))

  // WorkloadIdentityTokenProvider is absent from 3.3.4; Configuration.getClass raises the
  // RuntimeException the resolver reports (Hadoop itself would wrap it once more).
  private val fromWorkloadIdentity: Map[String, Pin] =
    from341(Pin.failed(ExceptionClasses.RUNTIME), Pin.resolved)

  def cases(credentialProviderPath: String): Seq[ParityCase] =
    uriCases ++ authTypeCases ++ sharedKeyCases ++ oauthCases ++ guardCases ++ sasCases ++
      credentialProviderCases(credentialProviderPath) ++ substitutionCases ++ calibrationCases

  private def c(
      id: String,
      group: String,
      note: String,
      conf: Map[String, String],
      uri: URI = URI_DFS,
      pins: Map[String, Pin] = Map.empty,
      liveValues: Map[String, String] = Map.empty): ParityCase =
    ParityCase(id, group, uri, conf, note, pins, liveValues)

  // ---- URI shapes ----

  private def uriCases: Seq[ParityCase] = {
    val blobAccount = "myacct.blob.core.windows.net"
    val portAccount = s"$ACCOUNT:8080"
    val upperAccount = "MyAcct.DFS.core.windows.net"
    val encodedContainer = "my%2Ddata"
    val secondAt = s"user@$ACCOUNT"
    Seq(
      c("uri-dfs-host", "uri", "plain dfs host", Map(acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT)),
      // hns.enabled=false: 3.4.2+ reject a Blob endpoint for an HNS account after authentication.
      c(
        "uri-blob-host",
        "uri",
        ".blob. host selects the BLOB service type on 3.4.2+",
        Map(
          acct(Keys.ACCOUNT_KEY, blobAccount) -> KEY_ACCOUNT,
          "fs.azure.account.hns.enabled" -> "false"),
        uri = uriFor(CONTAINER, blobAccount)),
      c(
        "uri-blob-in-path-only",
        "uri",
        ".blob. in the path also selects BLOB (Store checks the whole URI text)",
        Map(acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT, "fs.azure.account.hns.enabled" -> "false"),
        uri = new URI(s"abfss://$CONTAINER@$ACCOUNT/x.blob.y/file.parquet")),
      c(
        "uri-port-in-account",
        "uri",
        "the port is part of the account name Hadoop looks keys up under",
        Map(
          acct(Keys.ACCOUNT_KEY, portAccount) -> KEY_PORT,
          acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        uri = uriFor(CONTAINER, portAccount)),
      c(
        "uri-upper-case-account",
        "uri",
        "account name case is kept when matching keys",
        Map(acct(Keys.ACCOUNT_KEY, upperAccount) -> KEY_ACCOUNT),
        uri = uriFor(CONTAINER, upperAccount)),
      c(
        "uri-upper-case-account-lower-case-key",
        "uri",
        "a lower-case key does not configure a mixed-case account",
        Map(acct(Keys.ACCOUNT_KEY, upperAccount.toLowerCase(Locale.ROOT)) -> KEY_ACCOUNT),
        uri = uriFor(CONTAINER, upperAccount),
        pins = allVersions(Pin.unconfigured)),
      c(
        "uri-percent-encoded-container",
        "uri",
        "container-scoped keys match the raw (still encoded) container name",
        clientCreds ++ Map(cont(Keys.CLIENT_ID, encodedContainer) -> "container-client"),
        uri = uriFor(encodedContainer, ACCOUNT)),
      c(
        "uri-second-at",
        "uri",
        "everything after the first '@' is the account name",
        Map(acct(Keys.ACCOUNT_KEY, secondAt) -> KEY_ACCOUNT),
        uri = uriFor(CONTAINER, secondAt)),
      c(
        "uri-no-at",
        "uri",
        "no '@' in the authority",
        Map(Keys.ACCOUNT_KEY -> KEY_GLOBAL),
        uri = new URI(s"abfss://$ACCOUNT/path/file.parquet"),
        pins = allVersions(Pin.failed(ExceptionClasses.INVALID_URI_AUTHORITY))),
      c(
        "uri-empty-container",
        "uri",
        "empty container name",
        Map(Keys.ACCOUNT_KEY -> KEY_GLOBAL),
        uri = new URI(s"abfss://@$ACCOUNT/path/file.parquet"),
        pins = allVersions(Pin.failed(ExceptionClasses.INVALID_URI))),
      c(
        "uri-null-authority",
        "uri",
        "no authority at all",
        Map(Keys.ACCOUNT_KEY -> KEY_GLOBAL),
        uri = new URI("abfss:///path/file.parquet"),
        pins = allVersions(Pin.failed(ExceptionClasses.INVALID_URI_AUTHORITY))),
      c(
        "uri-account-without-dot",
        "uri",
        "SharedKey needs a fully qualified account name",
        Map(acct(Keys.ACCOUNT_KEY, "myacct") -> KEY_ACCOUNT),
        uri = uriFor(CONTAINER, "myacct"),
        pins = allVersions(Pin.failed(ExceptionClasses.INVALID_URI))))
  }

  // ---- auth type values and scopes ----

  private def authTypeCases: Seq[ParityCase] = {
    val oauthKeys = clientCreds - acct(Keys.AUTH_TYPE)
    val sharedKey = Map(acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT)
    val badEnum = Pin.failed(ExceptionClasses.ILLEGAL_ARGUMENT)
    def scoped(scope: String, key: String): String =
      if (scope == "global") key else acct(key)
    Seq("global", "account").flatMap { scope =>
      val at = scoped(scope, Keys.AUTH_TYPE)
      Seq(
        c(
          s"at-$scope-sharedkey",
          "auth-type",
          s"SharedKey ($scope)",
          sharedKey + (at -> "SharedKey")),
        c(s"at-$scope-oauth", "auth-type", s"OAuth ($scope)", oauthKeys + (at -> "OAuth")),
        c(
          s"at-$scope-sas-custom",
          "auth-type",
          s"SAS with a custom provider ($scope)",
          Map(at -> "SAS", acct(Keys.SAS_TOKEN_PROVIDER_TYPE) -> SAS_PROVIDER),
          pins = allVersions(Pin.declined)),
        c(
          s"at-$scope-custom",
          "auth-type",
          s"Custom ($scope)",
          Map(at -> "Custom", acct(Keys.OAUTH_PROVIDER_TYPE) -> CUSTOM_ADAPTEE),
          pins = allVersions(Pin.declined)),
        c(
          s"at-$scope-lower-case",
          "auth-type",
          s"'oauth' is not an enum constant ($scope)",
          oauthKeys + (at -> "oauth"),
          pins = allVersions(badEnum)),
        c(
          s"at-$scope-padded",
          "auth-type",
          s"' OAuth ' is trimmed by getEnum ($scope)",
          oauthKeys + (at -> " OAuth "),
          pins = allVersions(Pin.resolved)),
        c(
          s"at-$scope-empty",
          "auth-type",
          s"'' is not unset: Enum.valueOf fails ($scope)",
          oauthKeys + (at -> ""),
          pins = allVersions(badEnum)),
        c(
          s"at-$scope-userbound",
          "auth-type",
          s"UserboundSASWithOAuth exists from 3.5.0 ($scope)",
          Map(at -> "UserboundSASWithOAuth", acct(Keys.SAS_TOKEN_PROVIDER_TYPE) -> SAS_PROVIDER),
          pins = from350(badEnum, Pin.declined)))
    } ++ Seq(
      c(
        "at-account-oauth-global-sharedkey",
        "auth-type",
        "the account value wins over a valid global one",
        clientCreds + (Keys.AUTH_TYPE -> "SharedKey") + (Keys.ACCOUNT_KEY -> KEY_GLOBAL),
        pins = allVersions(Pin.resolved)),
      c(
        "at-unset-no-keys",
        "auth-type",
        "nothing configured for the account",
        Map("fs.azure.account.hns.enabled" -> "true"),
        pins = allVersions(Pin.unconfigured)),
      c(
        "at-unset-other-account-only",
        "auth-type",
        "another account's key does not configure this one",
        Map(acct(Keys.ACCOUNT_KEY, "other.dfs.core.windows.net") -> KEY_GLOBAL),
        pins = allVersions(Pin.unconfigured)))
  }

  // ---- SharedKey ----

  private def sharedKeyCases: Seq[ParityCase] = {
    val kpe = Pin.failed(ExceptionClasses.KEY_PROVIDER)
    val emptyKey = Pin.failed(ExceptionClasses.ILLEGAL_ARGUMENT)
    val badKeys = Seq(
      ("bad-length", "c2VjcmV", "length % 4 != 0", kpe),
      ("bad-char", "c2Vj!mV0", "a character outside the Base64 alphabet", kpe),
      ("padded", " c2VjcmV0 ", "whitespace is not trimmed, so the length is wrong", kpe),
      ("empty", "", "'' passes the Base64 check and fails in SharedKeyCredentials", emptyKey))
    val placements = Seq(("account", acct(Keys.ACCOUNT_KEY)), ("global", Keys.ACCOUNT_KEY))
    val validity = placements.flatMap { case (placement, key) =>
      c(
        s"sk-$placement-valid",
        "sharedkey",
        s"valid key ($placement)",
        Map(key -> KEY_ACCOUNT)) +:
        badKeys.map { case (suffix, value, note, pin) =>
          c(
            s"sk-$placement-$suffix",
            "sharedkey",
            s"$note ($placement)",
            Map(key -> value),
            pins = allVersions(pin))
        }
    }
    val providers = Seq(
      c(
        "sk-keyprovider-simple",
        "sharedkey",
        "explicit SimpleKeyProvider",
        Map(
          acct(Keys.KEY_PROVIDER) -> SIMPLE_KEY_PROVIDER,
          acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT)),
      c(
        "sk-keyprovider-unknown-class",
        "sharedkey",
        "keyprovider names a class that does not exist",
        Map(acct(Keys.KEY_PROVIDER) -> MISSING_CLASS, acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(kpe)),
      c(
        "sk-keyprovider-not-a-keyprovider",
        "sharedkey",
        "keyprovider names a class that is not a KeyProvider",
        Map(acct(Keys.KEY_PROVIDER) -> NOT_A_PROVIDER, acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(kpe)),
      c(
        "sk-keyprovider-empty",
        "sharedkey",
        "keyprovider '' is a class name Hadoop cannot load",
        Map(acct(Keys.KEY_PROVIDER) -> "", acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(kpe)),
      c(
        "sk-keyprovider-padded",
        "sharedkey",
        "keyprovider is read with get, which does not trim",
        Map(
          acct(Keys.KEY_PROVIDER) -> s" $SIMPLE_KEY_PROVIDER ",
          acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(kpe)),
      c(
        "sk-keyprovider-custom",
        "sharedkey",
        "a custom KeyProvider supplies the key; no fs.azure.account.key needed",
        Map(acct(Keys.KEY_PROVIDER) -> classOf[ParityKeyProvider].getName),
        pins = allVersions(Pin.resolved)),
      c(
        "sk-keyprovider-custom-null",
        "sharedkey",
        "a KeyProvider returning null is reported against the account name",
        Map(acct(Keys.KEY_PROVIDER) -> classOf[ParityNullKeyProvider].getName),
        pins = allVersions(Pin.failed(ExceptionClasses.CONFIGURATION_PROPERTY_NOT_FOUND))),
      c(
        "sk-shell-echo",
        "sharedkey",
        "ShellDecryptionKeyProvider runs /bin/echo, which returns the envelope",
        Map(
          acct(Keys.KEY_PROVIDER) -> SHELL_KEY_PROVIDER,
          acct(Keys.SHELL_KEY_PROVIDER_SCRIPT) -> "/bin/echo",
          acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(Pin.resolved)),
      c(
        "sk-shell-script-unset",
        "sharedkey",
        "ShellDecryptionKeyProvider without a script",
        Map(acct(Keys.KEY_PROVIDER) -> SHELL_KEY_PROVIDER, acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(kpe)),
      c(
        "sk-shell-script-missing",
        "sharedkey",
        "ShellDecryptionKeyProvider with a script that does not exist",
        Map(
          acct(Keys.KEY_PROVIDER) -> SHELL_KEY_PROVIDER,
          acct(Keys.SHELL_KEY_PROVIDER_SCRIPT) -> "/nonexistent/comet-decrypt.sh",
          acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(kpe)),
      c(
        "sk-account-beats-global",
        "sharedkey",
        "the account key wins over the global one",
        Map(acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT, Keys.ACCOUNT_KEY -> KEY_GLOBAL)),
      c(
        "sk-container-ignored-with-account",
        "sharedkey",
        "SimpleKeyProvider never reads the container-scoped key",
        Map(cont(Keys.ACCOUNT_KEY) -> KEY_CONTAINER, acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(Pin.resolved)))
    validity ++ providers
  }

  // ---- OAuth ----

  private def oauthCases: Seq[ParityCase] = {
    val tape = Pin.failed(ExceptionClasses.TOKEN_ACCESS_PROVIDER)
    def missing(base: Map[String, String], key: String, id: String, pin: Map[String, Pin]) =
      c(
        s"oauth-$id",
        "oauth",
        s"${key.stripPrefix("fs.azure.account.oauth2.")} missing",
        base - acct(key),
        pins = pin)

    Seq(
      // ClientCreds
      c("oauth-clientcreds", "oauth", "ClientCreds, all keys set", clientCreds),
      missing(clientCreds, Keys.CLIENT_ENDPOINT, "clientcreds-no-endpoint", allVersions(tape)),
      missing(clientCreds, Keys.CLIENT_ID, "clientcreds-no-client-id", allVersions(tape)),
      missing(clientCreds, Keys.CLIENT_SECRET, "clientcreds-no-secret", allVersions(tape)),
      c(
        "oauth-clientcreds-padded-endpoint",
        "oauth",
        "mandatory keys are forwarded untrimmed",
        clientCreds + (acct(Keys.CLIENT_ENDPOINT) -> s" $ENDPOINT ")),
      c(
        "oauth-clientcreds-container-secret",
        "oauth",
        "container-scoped secret wins on 3.4.2+, is ignored before",
        clientCreds + (cont(Keys.CLIENT_SECRET) -> "container-secret-value")),
      c(
        "oauth-clientcreds-global",
        "oauth",
        "ClientCreds configured globally",
        clientCreds.map { case (k, v) => k.stripSuffix(s".$ACCOUNT") -> v }),
      c(
        "oauth-clientcreds-mixed-scopes",
        "oauth",
        "account auth type and provider, global endpoint and secret",
        Map(
          acct(Keys.AUTH_TYPE) -> "OAuth",
          acct(Keys.OAUTH_PROVIDER_TYPE) -> Providers.CLIENT_CREDS,
          Keys.CLIENT_ENDPOINT -> ENDPOINT,
          acct(Keys.CLIENT_ID) -> CLIENT_ID,
          Keys.CLIENT_SECRET -> CLIENT_SECRET)),
      c(
        "oauth-clientcreds-other-account-secret",
        "oauth",
        "another account's secret is never forwarded",
        clientCreds + (acct(Keys.CLIENT_SECRET, "other.dfs.core.windows.net") -> "other-secret")),
      // UserPassword
      c("oauth-userpassword", "oauth", "UserPassword, all keys set", userPassword),
      missing(userPassword, Keys.USER_NAME, "userpassword-no-user", allVersions(tape)),
      missing(userPassword, Keys.USER_PASSWORD, "userpassword-no-password", allVersions(tape)),
      missing(userPassword, Keys.CLIENT_ENDPOINT, "userpassword-no-endpoint", allVersions(tape)),
      // MSI
      c("oauth-msi", "oauth", "MSI with tenant and client id, defaults for the rest", msi),
      c(
        "oauth-msi-custom-endpoint-authority",
        "oauth",
        "MSI endpoint and authority are trimmed; the authority gets a trailing slash",
        msi ++ Map(
          acct(Keys.MSI_ENDPOINT) -> " http://msi.example.net/token ",
          acct(Keys.MSI_AUTHORITY) -> " https://login.example.net ")),
      c(
        "oauth-msi-blank-endpoint",
        "oauth",
        "a blank MSI endpoint means the default",
        msi + (acct(Keys.MSI_ENDPOINT) -> "   ")),
      c(
        "oauth-msi-no-client-id",
        "oauth",
        "client id: mandatory on 3.3.4, optional from 3.4.1",
        msi - acct(Keys.CLIENT_ID),
        pins = from341(tape, Pin.resolved)),
      c(
        "oauth-msi-container-tenant",
        "oauth",
        "container-scoped MSI tenant on 3.4.2+",
        msi + (cont(Keys.MSI_TENANT) -> "container-tenant")),
      // RefreshToken
      c("oauth-refreshtoken", "oauth", "RefreshToken, all keys set", refreshToken),
      missing(refreshToken, Keys.REFRESH_TOKEN, "refreshtoken-no-token", allVersions(tape)),
      missing(refreshToken, Keys.CLIENT_ID, "refreshtoken-no-client-id", allVersions(tape)),
      c(
        "oauth-refreshtoken-custom-endpoint",
        "oauth",
        "refresh token endpoint is trimmed",
        refreshToken + (acct(
          Keys.REFRESH_TOKEN_ENDPOINT) -> " https://login.example.net/token ")),
      // WorkloadIdentity
      c(
        "oauth-wi",
        "oauth",
        "Workload Identity with the default token file",
        workloadIdentity,
        pins = fromWorkloadIdentity),
      c(
        "oauth-wi-no-tenant",
        "oauth",
        "Workload Identity tenant is mandatory",
        workloadIdentity - acct(Keys.MSI_TENANT),
        pins = from341(Pin.failed(ExceptionClasses.RUNTIME), tape)),
      c(
        "oauth-wi-no-client-id",
        "oauth",
        "Workload Identity client id is mandatory",
        workloadIdentity - acct(Keys.CLIENT_ID),
        pins = from341(Pin.failed(ExceptionClasses.RUNTIME), tape)),
      c(
        "oauth-wi-padded-token-file",
        "oauth",
        "token file is trimmed",
        workloadIdentity + (acct(Keys.TOKEN_FILE) -> " /mnt/tokens/token "),
        pins = fromWorkloadIdentity),
      c(
        "oauth-wi-container-token-file",
        "oauth",
        "container-scoped token file on 3.4.2+",
        workloadIdentity + (cont(Keys.TOKEN_FILE) -> "/mnt/tokens/container-token"),
        pins = fromWorkloadIdentity),
      c(
        "oauth-wi-blank-assertion-provider",
        "oauth",
        "a blank client assertion provider is ignored on 3.5.0",
        workloadIdentity + (acct(Keys.CLIENT_ASSERTION_PROVIDER_TYPE) -> "   "),
        pins = fromWorkloadIdentity),
      // provider class shapes
      c(
        "oauth-no-provider-type",
        "oauth",
        "OAuth without a provider class",
        clientCreds - acct(Keys.OAUTH_PROVIDER_TYPE),
        pins = allVersions(Pin.failed(ExceptionClasses.ILLEGAL_ARGUMENT))),
      c(
        "oauth-provider-not-built-in",
        "oauth",
        "an AccessTokenProvider outside the five built-ins is rejected by ==",
        clientCreds + (acct(Keys.OAUTH_PROVIDER_TYPE) -> FOREIGN_ACCESS_TOKEN_PROVIDER),
        pins = allVersions(Pin.failed(ExceptionClasses.ILLEGAL_ARGUMENT))),
      c(
        "oauth-provider-not-a-provider",
        "oauth",
        "a class that is not an AccessTokenProvider",
        clientCreds + (acct(Keys.OAUTH_PROVIDER_TYPE) -> NOT_A_PROVIDER),
        pins = allVersions(Pin.failed(ExceptionClasses.RUNTIME))),
      c(
        "oauth-provider-missing-class",
        "oauth",
        "a provider class that does not exist",
        clientCreds + (acct(Keys.OAUTH_PROVIDER_TYPE) -> MISSING_CLASS),
        pins = allVersions(Pin.failed(ExceptionClasses.RUNTIME))),
      c(
        "oauth-provider-empty",
        "oauth",
        "provider type '' is an unloadable class name",
        clientCreds + (acct(Keys.OAUTH_PROVIDER_TYPE) -> ""),
        pins = allVersions(Pin.failed(ExceptionClasses.RUNTIME))),
      c(
        "oauth-provider-padded",
        "oauth",
        "provider type is trimmed by Configuration.getClass",
        clientCreds + (acct(Keys.OAUTH_PROVIDER_TYPE) -> s" ${Providers.CLIENT_CREDS} ")),
      c(
        "oauth-custom-not-adaptee",
        "oauth",
        "Custom with a class that is not a CustomTokenProviderAdaptee: declined, Hadoop fails",
        Map(
          acct(Keys.AUTH_TYPE) -> "Custom",
          acct(Keys.OAUTH_PROVIDER_TYPE) -> FOREIGN_ACCESS_TOKEN_PROVIDER),
        pins = allVersions(Pin.declined)),
      c(
        "oauth-custom-no-provider-type",
        "oauth",
        "Custom without a provider class: declined, Hadoop fails",
        Map(acct(Keys.AUTH_TYPE) -> "Custom"),
        pins = allVersions(Pin.declined)))
  }

  // ---- auth-type guard: a global provider class needs a matching global auth type ----

  private def guardCases: Seq[ParityCase] = {
    def scopedKey(scope: String, key: String): String = if (scope == "global") key else acct(key)
    val oauthValues = Map(
      acct(Keys.CLIENT_ENDPOINT) -> ENDPOINT,
      acct(Keys.CLIENT_ID) -> CLIENT_ID,
      acct(Keys.CLIENT_SECRET) -> CLIENT_SECRET)
    val unresolvedProviderClass = Pin.failed(ExceptionClasses.ILLEGAL_ARGUMENT)
    val scopes = Seq(
      ("global", "global"),
      ("account", "account"),
      ("account", "global"),
      ("global", "account"))
    scopes.flatMap { case (authScope, providerScope) =>
      val guardFails = authScope == "account" && providerScope == "global"
      Seq(
        c(
          s"guard-oauth-$authScope-type-$providerScope-provider",
          "guard",
          s"$authScope auth type, $providerScope OAuth provider class",
          oauthValues ++ Map(
            scopedKey(authScope, Keys.AUTH_TYPE) -> "OAuth",
            scopedKey(providerScope, Keys.OAUTH_PROVIDER_TYPE) -> Providers.CLIENT_CREDS),
          pins = allVersions(if (guardFails) unresolvedProviderClass else Pin.resolved)),
        c(
          s"guard-sas-$authScope-type-$providerScope-provider",
          "guard",
          s"$authScope auth type, $providerScope SAS provider class",
          Map(
            scopedKey(authScope, Keys.AUTH_TYPE) -> "SAS",
            scopedKey(providerScope, Keys.SAS_TOKEN_PROVIDER_TYPE) -> SAS_PROVIDER),
          pins = if (guardFails) noSasSource else allVersions(Pin.declined)))
    }
  }

  // ---- SAS ----

  private def sasCases: Seq[ParityCase] = {
    val fixed = Map(acct(Keys.AUTH_TYPE) -> "SAS", acct(Keys.SAS_FIXED_TOKEN) -> FIXED_SAS)
    Seq(
      c("sas-fixed-only", "sas", "fixed token only", fixed, pins = fixedSasOnly),
      c(
        "sas-custom-only",
        "sas",
        "custom provider only",
        sasCustom,
        pins = allVersions(Pin.declined)),
      c(
        "sas-custom-and-fixed",
        "sas",
        "the custom provider wins over the fixed token",
        sasCustom + (acct(Keys.SAS_FIXED_TOKEN) -> FIXED_SAS),
        pins = allVersions(Pin.declined)),
      c(
        "sas-neither",
        "sas",
        "neither provider nor token",
        Map(acct(Keys.AUTH_TYPE) -> "SAS"),
        pins = noSasSource),
      c(
        "sas-blank-fixed",
        "sas",
        "a blank fixed token counts as unset",
        fixed + (acct(Keys.SAS_FIXED_TOKEN) -> "   "),
        pins = noSasSource),
      c(
        "sas-question-mark",
        "sas",
        "a leading '?' is kept at init; the client strips it per request",
        fixed + (acct(Keys.SAS_FIXED_TOKEN) -> s"?$FIXED_SAS"),
        pins = fixedSasOnly),
      c(
        "sas-fixed-global",
        "sas",
        "global auth type and fixed token",
        Map(Keys.AUTH_TYPE -> "SAS", Keys.SAS_FIXED_TOKEN -> FIXED_SAS),
        pins = fixedSasOnly),
      c(
        "sas-provider-not-a-provider",
        "sas",
        "a class that is not a SASTokenProvider",
        Map(acct(Keys.AUTH_TYPE) -> "SAS", acct(Keys.SAS_TOKEN_PROVIDER_TYPE) -> NOT_A_PROVIDER),
        pins = allVersions(Pin.failed(ExceptionClasses.RUNTIME))),
      c(
        "sas-provider-missing-class",
        "sas",
        "a provider class that does not exist",
        Map(acct(Keys.AUTH_TYPE) -> "SAS", acct(Keys.SAS_TOKEN_PROVIDER_TYPE) -> MISSING_CLASS),
        pins = allVersions(Pin.failed(ExceptionClasses.RUNTIME))))
  }

  // ---- credential providers (JCEKS) and the clear-text fallback ----

  private def credentialProviderCases(providerPath: String): Seq[ParityCase] = {
    val provider = Map("hadoop.security.credential.provider.path" -> providerPath)
    val noFallback = Map("hadoop.security.credential.clear-text-fallback" -> "false")
    val kpe = Pin.failed(ExceptionClasses.KEY_PROVIDER)
    Seq(
      c(
        "cp-jceks-account-key",
        "credential-provider",
        "the account key comes from the keystore; no clear-text key",
        provider,
        pins = allVersions(Pin.resolved)),
      c(
        "cp-jceks-beats-clear-text",
        "credential-provider",
        "the keystore is consulted before the clear-text value",
        provider + (acct(Keys.ACCOUNT_KEY) -> KEY_GLOBAL),
        pins = allVersions(Pin.resolved)),
      c(
        "cp-jceks-no-fallback",
        "credential-provider",
        "keystore with the clear-text fallback off",
        provider ++ noFallback,
        pins = allVersions(Pin.resolved)),
      c(
        "cp-jceks-client-secret",
        "credential-provider",
        "the OAuth client secret comes from the keystore",
        provider ++ (clientCreds - acct(Keys.CLIENT_SECRET)),
        pins = allVersions(Pin.resolved)),
      c(
        "cp-no-fallback-clear-text-only",
        "credential-provider",
        "fallback off, no keystore: a clear-text key is invisible, so nothing is configured",
        noFallback + (acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(Pin.unconfigured)),
      c(
        "cp-no-fallback-with-auth-type",
        "credential-provider",
        "fallback off, no keystore, SharedKey named: the key provider finds nothing",
        noFallback ++ Map(
          acct(Keys.AUTH_TYPE) -> "SharedKey",
          acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(kpe)))
  }

  // ---- ${...} substitution ----

  private def substitutionCases: Seq[ParityCase] = {
    // Built by concatenation so scalac does not read them as interpolations missing their prefix.
    val crossKey = "${" + "comet.parity.account.key}"
    val envWithDefault = "${env." + UNSET_ENV_VAR + ":-fallback-client}"
    val envHome = "${" + "env.HOME}"
    Seq(
      c(
        "subst-cross-key",
        "substitution",
        "a reference to another key resolves through Configuration.get",
        Map(acct(Keys.ACCOUNT_KEY) -> crossKey, "comet.parity.account.key" -> KEY_ENV),
        pins = allVersions(Pin.resolved)),
      c(
        "subst-env-default",
        "substitution",
        "an environment reference with a default falls back when the variable is unset",
        clientCreds + (acct(Keys.CLIENT_ID) -> envWithDefault),
        pins = allVersions(Pin.resolved)),
      c(
        "subst-env-home",
        "substitution",
        "an environment reference reads the variable",
        clientCreds + (acct(Keys.CLIENT_ID) -> envHome),
        pins = allVersions(Pin.resolved),
        liveValues = Map(Keys.CLIENT_ID -> sys.env.getOrElse("HOME", envHome))))
  }

  // ---- calibration: one case per version difference and per surprising Hadoop rule ----

  private def calibrationCases: Seq[ParityCase] = {
    val tape = Pin.failed(ExceptionClasses.TOKEN_ACCESS_PROVIDER)
    val kpe = Pin.failed(ExceptionClasses.KEY_PROVIDER)
    val badEnum = Pin.failed(ExceptionClasses.ILLEGAL_ARGUMENT)
    val portAccount = s"$ACCOUNT:8080"
    val prefixedEndpoint = "https://login.example.net/prefix/tenant-abc/oauth2/v2.0/token"
    val assertionProvider = "com.example.comet.NeverLoadedAssertionProvider"
    Seq(
      c(
        "cal-sas-guard-with-fixed-token",
        "calibration",
        "account SAS type + global SAS provider class: the guard drops the class, the token wins",
        Map(
          acct(Keys.AUTH_TYPE) -> "SAS",
          Keys.SAS_TOKEN_PROVIDER_TYPE -> SAS_PROVIDER,
          acct(Keys.SAS_FIXED_TOKEN) -> FIXED_SAS),
        pins = fixedSasOnly),
      c(
        "cal-sas-guard-without-fixed-token",
        "calibration",
        "account SAS type + global SAS provider class and no token: nothing is left",
        Map(acct(Keys.AUTH_TYPE) -> "SAS", Keys.SAS_TOKEN_PROVIDER_TYPE -> SAS_PROVIDER),
        pins = noSasSource),
      c(
        "cal-sas-container-fixed-token",
        "calibration",
        "container-scoped fixed token: honoured from 3.4.2, invisible on 3.4.1",
        Map(acct(Keys.AUTH_TYPE) -> "SAS", cont(Keys.SAS_FIXED_TOKEN) -> FIXED_SAS),
        pins = pins(
          Pin.failed(ExceptionClasses.TOKEN_ACCESS_PROVIDER),
          Pin.failed(ExceptionClasses.SAS_TOKEN_PROVIDER),
          Pin.resolved,
          Pin.resolved)),
      c(
        "cal-sas-untrimmed-token",
        "calibration",
        "the fixed token is trimmed before use",
        Map(acct(Keys.AUTH_TYPE) -> "SAS", acct(Keys.SAS_FIXED_TOKEN) -> s"  $FIXED_SAS  "),
        pins = fixedSasOnly),
      c(
        "cal-endpoint-with-path-prefix",
        "calibration",
        "the client endpoint is forwarded verbatim, whatever its path shape",
        clientCreds + (acct(Keys.CLIENT_ENDPOINT) -> prefixedEndpoint),
        pins = allVersions(Pin.resolved)),
      c(
        "cal-port-key-matches",
        "calibration",
        "with a port in the URI, only the port-qualified key configures the account",
        Map(acct(Keys.ACCOUNT_KEY, portAccount) -> KEY_PORT),
        uri = uriFor(CONTAINER, portAccount),
        pins = allVersions(Pin.resolved)),
      c(
        "cal-port-key-missing",
        "calibration",
        "with a port in the URI, the port-less key belongs to another account",
        Map(acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        uri = uriFor(CONTAINER, portAccount),
        pins = allVersions(Pin.unconfigured)),
      c(
        "cal-container-oauth-endpoint",
        "calibration",
        "container-scoped client endpoint: read from 3.4.2, missing before",
        (clientCreds - acct(Keys.CLIENT_ENDPOINT)) + (cont(Keys.CLIENT_ENDPOINT) -> ENDPOINT),
        pins = from342(tape, Pin.resolved)),
      c(
        "cal-container-account-key-only",
        "calibration",
        "a container-scoped account key alone: unread by SimpleKeyProvider on every version",
        Map(cont(Keys.ACCOUNT_KEY) -> KEY_CONTAINER),
        pins = from342(Pin.unconfigured, kpe)),
      c(
        "cal-global-enum-parsed-first",
        "calibration",
        "a bad global auth type fails even when the account one is valid",
        clientCreds + (Keys.AUTH_TYPE -> "oauth"),
        pins = allVersions(badEnum)),
      c(
        "cal-empty-mandatory-secret",
        "calibration",
        "'' passes the mandatory getter and is forwarded",
        clientCreds + (acct(Keys.CLIENT_SECRET) -> ""),
        pins = allVersions(Pin.resolved)),
      c(
        "cal-msi-no-tenant",
        "calibration",
        "MSI tenant: mandatory on 3.3.4, optional from 3.4.1",
        msi - acct(Keys.MSI_TENANT),
        pins = from341(tape, Pin.resolved)),
      c(
        "cal-msi-no-tenant-no-client-id",
        "calibration",
        "MSI with neither tenant nor client id",
        msi - acct(Keys.MSI_TENANT) - acct(Keys.CLIENT_ID),
        pins = from341(tape, Pin.resolved)),
      c(
        "cal-null-fsname-key",
        "calibration",
        "SimpleKeyProvider's 2-arg configuration probes the literal key.null.<account> on 3.4.2+",
        Map(s"${Keys.ACCOUNT_KEY}.null.$ACCOUNT" -> KEY_NULL_LITERAL),
        pins = from342(Pin.unconfigured, Pin.resolved)),
      c(
        "cal-null-fsname-key-beats-account",
        "calibration",
        "key.null.<account> is the first level on 3.4.2+, so it wins over the account key",
        Map(
          s"${Keys.ACCOUNT_KEY}.null.$ACCOUNT" -> KEY_NULL_LITERAL,
          acct(Keys.ACCOUNT_KEY) -> KEY_ACCOUNT),
        pins = allVersions(Pin.resolved)),
      c(
        "cal-base64-bad-length",
        "calibration",
        "Base64 validation: length % 4 != 0",
        Map(acct(Keys.ACCOUNT_KEY) -> "c2VjcmV0c2V"),
        pins = allVersions(kpe)),
      c(
        "cal-base64-non-ascii",
        "calibration",
        "Base64 validation: a char >= U+0080 escapes as ArrayIndexOutOfBoundsException",
        Map(acct(Keys.ACCOUNT_KEY) -> "c2Vjcm\u00e90"),
        pins = allVersions(Pin.failed(ExceptionClasses.ARRAY_INDEX))),
      c(
        "cal-empty-account-key",
        "calibration",
        "'' passes Base64 validation and fails in SharedKeyCredentials",
        Map(acct(Keys.ACCOUNT_KEY) -> ""),
        pins = allVersions(Pin.failed(ExceptionClasses.ILLEGAL_ARGUMENT))),
      c(
        "cal-wi-on-334",
        "calibration",
        "WorkloadIdentityTokenProvider does not exist on 3.3.4",
        workloadIdentity,
        pins = fromWorkloadIdentity),
      c(
        "cal-wi-client-assertion-provider",
        "calibration",
        "3.5.0 Workload Identity with a client assertion provider is declined, never loaded",
        workloadIdentity + (acct(Keys.CLIENT_ASSERTION_PROVIDER_TYPE) -> s" $assertionProvider "),
        pins =
          pins(Pin.failed(ExceptionClasses.RUNTIME), Pin.resolved, Pin.resolved, Pin.declined)),
      c(
        "cal-userbound-sas",
        "calibration",
        "UserboundSASWithOAuth: an enum constant only from 3.5.0",
        Map(
          acct(Keys.AUTH_TYPE) -> "UserboundSASWithOAuth",
          acct(Keys.SAS_TOKEN_PROVIDER_TYPE) -> SAS_PROVIDER),
        pins = from350(badEnum, Pin.declined)),
      c(
        "cal-oauth-keys-without-auth-type",
        "calibration",
        "OAuth keys without an auth type are a SharedKey misconfiguration, not 'unconfigured'",
        clientCreds - acct(Keys.AUTH_TYPE),
        pins = allVersions(kpe)))
  }
}

/** A `KeyProvider` that supplies a fixed key; stands in for a vault-backed provider. */
class ParityKeyProvider extends KeyProvider {
  override def getStorageAccountKey(accountName: String, conf: Configuration): String =
    AbfsAuthParityCases.KEY_CUSTOM
}

/** A `KeyProvider` that finds nothing: Hadoop reports the account as a missing property. */
class ParityNullKeyProvider extends KeyProvider {
  override def getStorageAccountKey(accountName: String, conf: Configuration): String = null
}

/** A `CustomTokenProviderAdaptee` that initializes fine, so a Custom FileSystem init succeeds. */
class ParityCustomTokenAdaptee extends CustomTokenProviderAdaptee {
  override def initialize(configuration: Configuration, accountName: String): Unit = ()
  override def getAccessToken(): String = "parity-token"
  override def getExpiryTime(): Date = new Date(Long.MaxValue)
}

/** A `SASTokenProvider` that initializes fine, so a SAS FileSystem init succeeds. */
class ParitySasTokenProvider extends SASTokenProvider {
  override def initialize(configuration: Configuration, accountName: String): Unit = ()
  override def getSASToken(
      account: String,
      fileSystem: String,
      path: String,
      operation: String): String = AbfsAuthParityCases.FIXED_SAS
}

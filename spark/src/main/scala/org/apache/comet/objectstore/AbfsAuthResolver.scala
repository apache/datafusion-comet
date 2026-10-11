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

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging

import org.apache.comet.objectstore.AbfsReflection.Handles

/**
 * Resolves the ABFS authentication for one `abfs[s]://container@account/...` URI on the driver,
 * by asking the classpath's hadoop-azure the questions
 * `AzureBlobFileSystemStore.initializeClient` asks, and encodes the answer as the options the
 * native Azure store reads.
 *
 * Hadoop's `AbfsConfiguration` decides which mechanism applies and where each value comes from
 * (container-, account- or globally-scoped keys, credential providers, `${...}` references). The
 * resolver forwards the resolved values under the global key names plus `comet.azure.*` markers,
 * never a key for another account, and never the environment. Anything Hadoop throws becomes an
 * error marker: the native scan fails with it instead of trying a lookup of its own.
 *
 * `AbfsAuthParitySuite` pins the outcome of every configuration shape per hadoop-azure version
 * (`spark/src/test/resources/abfs-auth-parity/expected-<version>.json`) and cross-checks it
 * against the real `AzureBlobFileSystem`; its scaladoc says how to regenerate those files after a
 * deliberate change here.
 */
private[comet] object AbfsAuthResolver extends Logging {

  /** Options the native Azure store reads. None of them starts with `fs.`. */
  object Markers {
    val RESOLUTION = "comet.azure.resolution"
    val AUTH_TYPE = "comet.azure.auth.type"
    val OAUTH_PROVIDER_CLASS = "comet.azure.oauth.provider.class"
    val SAS_PROVIDER_CLASS = "comet.azure.sas.provider.class"
    val OAUTH_ASSERTION_PROVIDER_CLASS = "comet.azure.oauth.assertion.provider.class"
    val ERROR_CLASS = "comet.azure.error.class"
    val ERROR_MESSAGE = "comet.azure.error.message"

    val RESOLVED = "resolved"
    val NONE = "none"
    val DECLINED = "declined"
    val ERROR = "error"
  }

  /** Hadoop ABFS configuration keys, global form (`ConfigurationKeys`). */
  object Keys {
    val AUTH_TYPE = "fs.azure.account.auth.type"
    val ACCOUNT_KEY = "fs.azure.account.key"
    val KEY_PROVIDER = "fs.azure.account.keyprovider"
    val SHELL_KEY_PROVIDER_SCRIPT = "fs.azure.shellkeyprovider.script"
    val OAUTH_PROVIDER_TYPE = "fs.azure.account.oauth.provider.type"
    val CLIENT_ID = "fs.azure.account.oauth2.client.id"
    val CLIENT_SECRET = "fs.azure.account.oauth2.client.secret"
    val CLIENT_ENDPOINT = "fs.azure.account.oauth2.client.endpoint"
    val MSI_TENANT = "fs.azure.account.oauth2.msi.tenant"
    val MSI_ENDPOINT = "fs.azure.account.oauth2.msi.endpoint"
    val MSI_AUTHORITY = "fs.azure.account.oauth2.msi.authority"
    val USER_NAME = "fs.azure.account.oauth2.user.name"
    val USER_PASSWORD = "fs.azure.account.oauth2.user.password"
    val REFRESH_TOKEN = "fs.azure.account.oauth2.refresh.token"
    val REFRESH_TOKEN_ENDPOINT = "fs.azure.account.oauth2.refresh.token.endpoint"
    val TOKEN_FILE = "fs.azure.account.oauth2.token.file"
    val CLIENT_ASSERTION_PROVIDER_TYPE = "fs.azure.account.oauth2.client.assertion.provider.type"
    val SAS_TOKEN_PROVIDER_TYPE = "fs.azure.sas.token.provider.type"
    val SAS_FIXED_TOKEN = "fs.azure.sas.fixed.token"
  }

  /** The OAuth providers Hadoop accepts under `AuthType.OAuth`, matched by exact class name. */
  object Providers {
    val CLIENT_CREDS = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    val USER_PASSWORD = "org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider"
    val MSI = "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
    val REFRESH_TOKEN = "org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider"
    val WORKLOAD_IDENTITY = "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider"
    val BUILT_IN: Set[String] =
      Set(CLIENT_CREDS, USER_PASSWORD, MSI, REFRESH_TOKEN, WORKLOAD_IDENTITY)
  }

  private object AuthTypes {
    val SHARED_KEY = "SharedKey"
    val OAUTH = "OAuth"
    val SAS = "SAS"
  }

  private object Exceptions {
    private val pkg = "org.apache.hadoop.fs.azurebfs.contracts.exceptions."
    val CONFIGURATION_PROPERTY_NOT_FOUND: String = pkg + "ConfigurationPropertyNotFoundException"
    val INVALID_CONFIGURATION_VALUE: String = pkg + "InvalidConfigurationValueException"
    val INVALID_URI: String = pkg + "InvalidUriException"
    val INVALID_URI_AUTHORITY: String = pkg + "InvalidUriAuthorityException"
  }

  /**
   * Exceptions whose messages name keys, classes or URIs and nothing else, so they are forwarded
   * verbatim. Every other message (a `KeyProviderException` quotes the raw account key, custom
   * providers say anything) is replaced by a pointer to the driver log.
   */
  private val verbatimMessageClasses: Set[String] = Set(
    Exceptions.CONFIGURATION_PROPERTY_NOT_FOUND,
    Exceptions.INVALID_CONFIGURATION_VALUE,
    Exceptions.INVALID_URI,
    Exceptions.INVALID_URI_AUTHORITY,
    classOf[ClassNotFoundException].getName,
    classOf[NoSuchMethodException].getName,
    classOf[NoSuchFieldException].getName)

  // An IllegalArgumentException is verbatim only when Hadoop or the JDK enum parser raised it
  // ("No enum constant", "Failed to initialize", "Invalid account key."). One thrown
  // by a user KeyProvider could carry anything, so it is reported by class only.
  private val HADOOP_AZURE_PACKAGE = "org.apache.hadoop.fs.azurebfs."
  private val JAVA_ENUM_CLASS = "java.lang.Enum"

  private def isVerbatim(t: Throwable): Boolean = t match {
    case _: IllegalArgumentException =>
      t.getStackTrace.headOption.map(_.getClassName).exists { origin =>
        origin == JAVA_ENUM_CLASS || origin.startsWith(HADOOP_AZURE_PACKAGE)
      }
    case _ => verbatimMessageClasses.contains(t.getClass.getName)
  }

  private val REDACTED = "<redacted>"
  // Shorter values (`true`, a port) would mangle messages without protecting anything.
  private val MIN_REDACTED_LENGTH = 8
  // `SimpleKeyProvider` (3.4.1+) puts the raw account key in its message as `key ="..."`.
  private val QUOTED_KEY_PATTERN = "key =\"[^\"]*\"".r
  private val MAX_CAUSE_DEPTH = 8

  // The keys that name a mechanism or carry a credential in hadoop-azure. A configuration where
  // none of them resolves for the account is `Unconfigured`; any one of them hands the decision to
  // Hadoop, which defaults to SharedKey and fails in its own words when that does not fit.
  private[objectstore] val plainAuthKeys: Seq[String] = Seq(
    Keys.AUTH_TYPE,
    Keys.KEY_PROVIDER,
    Keys.SHELL_KEY_PROVIDER_SCRIPT,
    Keys.OAUTH_PROVIDER_TYPE,
    Keys.SAS_TOKEN_PROVIDER_TYPE)
  private[objectstore] val passwordAuthKeys: Seq[String] = Seq(
    Keys.ACCOUNT_KEY,
    Keys.CLIENT_ID,
    Keys.CLIENT_SECRET,
    Keys.CLIENT_ENDPOINT,
    Keys.MSI_TENANT,
    Keys.MSI_ENDPOINT,
    Keys.MSI_AUTHORITY,
    Keys.USER_NAME,
    Keys.USER_PASSWORD,
    Keys.REFRESH_TOKEN,
    Keys.REFRESH_TOKEN_ENDPOINT,
    Keys.TOKEN_FILE,
    Keys.CLIENT_ASSERTION_PROVIDER_TYPE,
    Keys.SAS_FIXED_TOKEN)

  sealed trait Outcome

  /** Hadoop named a mechanism and every value it needs; `values` use the global key names. */
  final case class Resolved(
      authType: String,
      providerClass: Option[String],
      values: Map[String, String])
      extends Outcome

  /** No auth-bearing key resolves for the account. */
  case object Unconfigured extends Outcome

  /**
   * Hadoop named a mechanism the native scan cannot run; `providerClass` was never instantiated.
   * `assertionProviderClass` is the trimmed value of
   * `fs.azure.account.oauth2.client.assertion.provider.type` that a Workload Identity
   * configuration names on hadoop-azure 3.5.0 and later.
   */
  final case class Declined(
      authType: String,
      providerClass: Option[String],
      assertionProviderClass: Option[String] = None)
      extends Outcome

  /** Hadoop (or the reflection onto it) threw; `message` is already redacted. */
  final case class Failed(exceptionClass: String, message: String) extends Outcome

  def resolve(hadoopConf: Configuration, uri: URI): Outcome =
    resolve(hadoopConf, uri, AbfsReflection.handles(AbfsReflection.loaderFor(hadoopConf)))

  private[objectstore] def resolve(
      hadoopConf: Configuration,
      uri: URI,
      handles: Try[Handles]): Outcome = {
    val secrets = ArrayBuffer[String]()
    // Set once the AbfsConfiguration exists, so a failure can read the credentials it skipped.
    var openSession: Option[Session] = None
    try {
      handles match {
        case Failure(e) => failed(uri, e, secrets)
        case Success(h) =>
          authorityParts(uri) match {
            case Left(failure) => failure
            case Right((container, account)) =>
              val session = new Session(
                h,
                h.newAbfsConfiguration(hadoopConf, account, container, uri),
                hadoopConf,
                account,
                secrets)
              openSession = Some(session)
              if (!session.isConfigured) {
                Unconfigured
              } else {
                val authType: Enum[_] = h.getAuthType(session.abfsConf, account)
                authType.name match {
                  case AuthTypes.SHARED_KEY => sharedKey(session, uri, account)
                  case AuthTypes.SAS => sas(session, authType)
                  case AuthTypes.OAUTH => oauth(session, authType)
                  // Custom and UserboundSASWithOAuth run Java providers per request; never
                  // instantiated here.
                  case other => Declined(other, None)
                }
              }
          }
      }
    } catch {
      // NonFatal alone misses NoSuchMethodError and friends, the reflection failures this must
      // report as an error marker rather than let escape into planning.
      case e @ (_: LinkageError | NonFatal(_)) =>
        openSession.foreach(_.recordSkippedSecrets())
        failed(uri, e, secrets)
    }
  }

  /**
   * `(container, account)` exactly as `AzureBlobFileSystemStore.authorityParts`: the raw
   * authority split once at the first `@`, so port, case and percent-encoding are kept. Failures
   * carry Hadoop's exception class and message.
   */
  private[objectstore] def authorityParts(uri: URI): Either[Failed, (String, String)] = {
    Option(uri.getRawAuthority) match {
      case None =>
        Left(Failed(Exceptions.INVALID_URI_AUTHORITY, s"$uri has invalid authority."))
      case Some(authority) if !authority.contains("@") =>
        Left(Failed(Exceptions.INVALID_URI_AUTHORITY, s"$uri has invalid authority."))
      case Some(authority) =>
        val parts = authority.split("@", 2)
        if (parts.length < 2 || parts(0).isEmpty) {
          Left(
            Failed(
              Exceptions.INVALID_URI,
              s"Invalid URI '$uri' has a malformed authority, expected container name. " +
                "Authority takes the form abfs://[<container name>@]<account name>"))
        } else {
          Right((parts(0), parts(1)))
        }
    }
  }

  // One resolution's view of an AbfsConfiguration; records every credential it reads so a
  // failure message can be redacted against them. Each password read may load a credential
  // provider's keystore, so a successful resolution reads only what the mechanism needs.
  private final class Session(
      val h: Handles,
      val abfsConf: AnyRef,
      hadoopConf: Configuration,
      account: String,
      secrets: ArrayBuffer[String]) {

    def password(key: String): Option[String] = {
      val value = h.getPasswordString(abfsConf, key)
      value.foreach(v => recordSecret(v))
      value
    }

    // Both spellings, since Hadoop quotes the raw value and the providers use the trimmed one.
    private def recordSecret(value: String): Unit =
      Seq(value, value.trim).filter(_.length >= MIN_REDACTED_LENGTH).foreach(secrets += _)

    /** `getTrimmedPasswordString`: blank means the default, then trimmed. */
    def trimmedPassword(key: String, default: String): String =
      password(key).filterNot(StringUtils.isBlank).getOrElse(default).trim

    def storageAccountKey(): String = {
      val key = h.getStorageAccountKey(abfsConf)
      recordSecret(key)
      key
    }

    // Stops at the first key that is set, plain keys first since they need no password lookup.
    def isConfigured: Boolean =
      plainAuthKeys.exists(isSet(abfsConf, _, password = false)) ||
        passwordAuthKeys.exists(isSet(abfsConf, _, password = true)) ||
        hasKeyProviderAccountKey

    /**
     * Reads every credential key the resolution may have skipped, so the failure message is
     * redacted against all of them. A read that throws is ignored: the failure being reported is
     * the one that matters.
     */
    def recordSkippedSecrets(): Unit = {
      passwordAuthKeys.foreach(key => bestEffort(isSet(abfsConf, key, password = true)))
      bestEffort(hasKeyProviderAccountKey)
    }

    private def bestEffort(read: => Boolean): Unit =
      try {
        read
        ()
      } catch {
        case _: LinkageError | NonFatal(_) => ()
      }

    // A value Hadoop cannot read still counts as set: Hadoop reads a key only when the mechanism
    // it picks needs it, and then fails in its own words.
    private def isSet(conf: AnyRef, key: String, password: Boolean): Boolean =
      h.read(conf, key, password) match {
        case Right(value) =>
          if (password) value.foreach(v => recordSecret(v))
          value.isDefined
        case Left(_) => true
      }

    // SimpleKeyProvider reads the key through its own two-argument AbfsConfiguration, whose
    // container level on 3.4.2+ is the literal `key.null.<account>`; a key placed there
    // configures the account for Hadoop, so it counts here too.
    private def hasKeyProviderAccountKey: Boolean =
      h.hasContainerScopedConfiguration && isSet(
        h.newKeyProviderConfiguration(hadoopConf, account),
        Keys.ACCOUNT_KEY,
        password = true)
  }

  private def sharedKey(session: Session, uri: URI, account: String): Outcome = {
    // Store.initializeClient checks the account name before fetching the key.
    if (account.indexOf('.') <= 0) {
      Failed(Exceptions.INVALID_URI, s"Invalid URI $uri - account name is not fully qualified.")
    } else {
      val key = session.storageAccountKey()
      if (key.isEmpty) {
        // SharedKeyCredentials rejects the empty key SimpleKeyProvider's Base64 check let through.
        Failed(classOf[IllegalArgumentException].getName, "Invalid account key.")
      } else {
        Resolved(AuthTypes.SHARED_KEY, None, Map(Keys.ACCOUNT_KEY -> key))
      }
    }
  }

  private def sas(session: Session, authType: Enum[_]): Outcome = {
    val h = session.h
    h.getTokenProviderClass(
      session.abfsConf,
      authType,
      Keys.SAS_TOKEN_PROVIDER_TYPE,
      h.sasTokenProviderClass) match {
      case Some(custom) => Declined(AuthTypes.SAS, Some(custom.getName))
      case None =>
        // Hadoop's own checks: 3.3.4 has no fixed token and fails here, 3.4.1+ requires one.
        h.getSASTokenProvider(session.abfsConf)
        val fixed = session.trimmedPassword(Keys.SAS_FIXED_TOKEN, "")
        Resolved(AuthTypes.SAS, None, Map(Keys.SAS_FIXED_TOKEN -> fixed))
    }
  }

  private def oauth(session: Session, authType: Enum[_]): Outcome = {
    val h = session.h
    val providerClass = h.getTokenProviderClass(
      session.abfsConf,
      authType,
      Keys.OAUTH_PROVIDER_TYPE,
      h.accessTokenProviderClass)
    val className = providerClass.map(_.getName)
    val assertionProvider =
      if (h.hasClientAssertionProvider && className.contains(Providers.WORKLOAD_IDENTITY)) {
        session.password(Keys.CLIENT_ASSERTION_PROVIDER_TYPE).map(_.trim).filter(_.nonEmpty)
      } else {
        None
      }
    assertionProvider match {
      case Some(assertion) =>
        // 3.5.0 Workload Identity with a custom ClientAssertionProvider: user code per request.
        Declined(AuthTypes.OAUTH, className, Some(assertion))
      case None =>
        // Hadoop validates the configuration and throws in its own words: an unresolved class,
        // a class that is not one of its five built-ins (matched by `==`, never instantiated),
        // a missing mandatory key. The built-in constructors do no I/O.
        h.getTokenProvider(session.abfsConf)
        Resolved(AuthTypes.OAUTH, className, oauthValues(session, className.getOrElse("")))
    }
  }

  // The keys each built-in provider's constructor receives, in the form Hadoop hands them over:
  // mandatory and plain keys verbatim, defaulted keys trimmed with the AuthConfigurations default.
  private def oauthValues(session: Session, providerClass: String): Map[String, String] = {
    val h = session.h
    def plain(key: String): Map[String, String] = session.password(key).map(key -> _).toMap
    def defaulted(key: String, defaultField: String): Map[String, String] =
      Map(key -> session.trimmedPassword(key, h.authDefault(defaultField)))
    def authority(): Map[String, String] = {
      val value = session.trimmedPassword(
        Keys.MSI_AUTHORITY,
        h.authDefault("DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY"))
      Map(Keys.MSI_AUTHORITY -> appendSlashIfNeeded(value))
    }

    providerClass match {
      case Providers.CLIENT_CREDS =>
        plain(Keys.CLIENT_ENDPOINT) ++ plain(Keys.CLIENT_ID) ++ plain(Keys.CLIENT_SECRET)
      case Providers.USER_PASSWORD =>
        plain(Keys.CLIENT_ENDPOINT) ++ plain(Keys.USER_NAME) ++ plain(Keys.USER_PASSWORD)
      case Providers.MSI =>
        defaulted(Keys.MSI_ENDPOINT, "DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT") ++
          plain(Keys.MSI_TENANT) ++ plain(Keys.CLIENT_ID) ++ authority()
      case Providers.REFRESH_TOKEN =>
        defaulted(
          Keys.REFRESH_TOKEN_ENDPOINT,
          "DEFAULT_FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN_ENDPOINT") ++
          plain(Keys.REFRESH_TOKEN) ++ plain(Keys.CLIENT_ID)
      case Providers.WORKLOAD_IDENTITY =>
        authority() ++ plain(Keys.MSI_TENANT) ++ plain(Keys.CLIENT_ID) ++
          defaulted(Keys.TOKEN_FILE, "DEFAULT_FS_AZURE_ACCOUNT_OAUTH_TOKEN_FILE")
      case _ => Map.empty
    }
  }

  private def appendSlashIfNeeded(authority: String): String =
    if (authority.endsWith("/")) authority else authority + "/"

  /**
   * The options for the native store: markers, plus the resolved values for `Resolved`. The
   * provider marker is `comet.azure.sas.provider.class` for SAS and
   * `comet.azure.oauth.provider.class` otherwise; a Workload Identity decline over a custom
   * client assertion keeps the provider marker and adds
   * `comet.azure.oauth.assertion.provider.class`.
   */
  def toOptions(outcome: Outcome): Map[String, String] = outcome match {
    case Resolved(authType, providerClass, values) =>
      Map(Markers.RESOLUTION -> Markers.RESOLVED, Markers.AUTH_TYPE -> authType) ++
        providerClass.map(providerMarker(authType) -> _) ++ values
    case Unconfigured =>
      Map(Markers.RESOLUTION -> Markers.NONE)
    case Declined(authType, providerClass, assertionProviderClass) =>
      Map(Markers.RESOLUTION -> Markers.DECLINED, Markers.AUTH_TYPE -> authType) ++
        providerClass.map(providerMarker(authType) -> _) ++
        assertionProviderClass.map(Markers.OAUTH_ASSERTION_PROVIDER_CLASS -> _)
    case Failed(exceptionClass, message) =>
      Map(
        Markers.RESOLUTION -> Markers.ERROR,
        Markers.ERROR_CLASS -> exceptionClass,
        Markers.ERROR_MESSAGE -> message)
  }

  private def providerMarker(authType: String): String =
    if (authType == AuthTypes.SAS) Markers.SAS_PROVIDER_CLASS else Markers.OAUTH_PROVIDER_CLASS

  /**
   * Hides every `secret` of at least [[MIN_REDACTED_LENGTH]] characters in `message`, and the raw
   * key a `SimpleKeyProvider` message quotes.
   */
  private[objectstore] def redact(message: String, secrets: Iterable[String]): String = {
    val withoutQuotedKey = QUOTED_KEY_PATTERN.replaceAllIn(message, "key =\"" + REDACTED + "\"")
    secrets
      .filter(_.length >= MIN_REDACTED_LENGTH)
      .foldLeft(withoutQuotedKey)((text, secret) => text.replace(secret, REDACTED))
  }

  private def failed(uri: URI, e: Throwable, secrets: Iterable[String]): Failed = {
    val chain = causeChain(e)
    val thrown = chain.head
    def describe(t: Throwable, verbatim: Boolean): String = {
      val name = t.getClass.getName
      if (verbatim) s"$name: ${redact(Option(t.getMessage).getOrElse(""), secrets)}" else name
    }
    logWarning(
      s"Hadoop ABFS authentication for ${uri.getScheme}://${uri.getRawAuthority} failed on the " +
        s"driver: ${chain.map(describe(_, verbatim = true)).mkString(", caused by ")}")

    val forwarded = chain.map(t => describe(t, isVerbatim(t)))
    val hidden = chain.exists(t => !isVerbatim(t))
    val pointer = if (hidden) "; the full message is in the Spark driver log" else ""
    val message = thrown match {
      case _: ClassNotFoundException =>
        "hadoop-azure is not on the driver classpath; the native ABFS scan resolves " +
          s"authentication through it (${forwarded.mkString(", caused by ")})"
      case _ => forwarded.mkString(", caused by ") + pointer
    }
    Failed(thrown.getClass.getName, message)
  }

  // KeyProviderException drops its cause, so its own message is all there is at that level.
  private def causeChain(e: Throwable): Seq[Throwable] = {
    val chain = ArrayBuffer[Throwable](e)
    var cause = e.getCause
    while (cause != null && chain.length < MAX_CAUSE_DEPTH && !chain.contains(cause)) {
      chain += cause
      cause = cause.getCause
    }
    chain.toSeq
  }
}

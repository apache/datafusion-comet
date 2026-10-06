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

import java.lang.reflect.{Constructor, InvocationTargetException, Method}
import java.net.URI

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.comet.util.ClassLoaders

/**
 * Reflective access to the hadoop-azure on the classpath. Comet does not link against
 * hadoop-azure; every version-dependent surface of `AbfsConfiguration` is resolved here, once per
 * class loader, so the resolver in [[AbfsAuthResolver]] can ask Hadoop the same questions
 * `AzureBlobFileSystemStore.initializeClient` asks.
 */
private[objectstore] object AbfsReflection {

  object ClassNames {
    val ABFS_CONFIGURATION = "org.apache.hadoop.fs.azurebfs.AbfsConfiguration"
    val ABFS_SERVICE_TYPE = "org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType"
    val AUTH_CONFIGURATIONS = "org.apache.hadoop.fs.azurebfs.constants.AuthConfigurations"
    val AUTH_TYPE = "org.apache.hadoop.fs.azurebfs.services.AuthType"
    val ACCESS_TOKEN_PROVIDER = "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider"
    val SAS_TOKEN_PROVIDER = "org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider"
    // Present from hadoop-azure 3.4.1: the fixed SAS token path.
    val FIXED_SAS_TOKEN_PROVIDER = "org.apache.hadoop.fs.azurebfs.services.FixedSASTokenProvider"
    // Present from hadoop-azure 3.5.0: Workload Identity with a custom client assertion.
    val CLIENT_ASSERTION_PROVIDER = "org.apache.hadoop.fs.azurebfs.oauth2.ClientAssertionProvider"
  }

  // Store.getAbfsServiceTypeFromUrl: BLOB when the URI text contains this, DFS otherwise.
  private val BLOB_DOMAIN_MARKER = ".blob."

  /**
   * The operations the resolver needs from an `AbfsConfiguration`. A `Configuration` object is
   * passed as `AnyRef` because its class is only known at run time. Hadoop's own exceptions
   * propagate unwrapped from every method.
   */
  trait Handles {

    /** True when hadoop-azure takes the container name (the 3.4.2+ four-argument constructor). */
    def hasContainerScopedConfiguration: Boolean

    /** True when `fs.azure.sas.fixed.token` exists (hadoop-azure 3.4.1+). */
    def hasFixedSasTokenProvider: Boolean

    /** True when a Workload Identity client assertion provider can be configured (3.5.0+). */
    def hasClientAssertionProvider: Boolean

    def accessTokenProviderClass: Class[_]

    def sasTokenProviderClass: Class[_]

    /**
     * `new AbfsConfiguration(conf, account, container, serviceType)`, or the two-argument form.
     */
    def newAbfsConfiguration(
        conf: Configuration,
        account: String,
        container: String,
        uri: URI): AnyRef

    /** `getAuthType(account)`: the `AuthType` enum constant. */
    def getAuthType(abfsConf: AnyRef, account: String): Enum[_]

    /** `get(key)`: account-specific, then global. */
    def get(abfsConf: AnyRef, key: String): Option[String]

    /**
     * `getPasswordString(key)`: container (3.4.2+), account, then global, via credential
     * providers.
     */
    def getPasswordString(abfsConf: AnyRef, key: String): Option[String]

    /**
     * `getTokenProviderClass(authType, key, null, xface)`: the provider class Hadoop would load.
     */
    def getTokenProviderClass(
        abfsConf: AnyRef,
        authType: Enum[_],
        key: String,
        xface: Class[_]): Option[Class[_]]

    /** `getStorageAccountKey()`: runs the configured `KeyProvider`, as Hadoop does at FS init. */
    def getStorageAccountKey(abfsConf: AnyRef): String

    /** `getTokenProvider()`, called only for Hadoop's validation of a built-in OAuth provider. */
    def getTokenProvider(abfsConf: AnyRef): Unit

    /** `getSASTokenProvider()`, called only for Hadoop's validation of the fixed SAS token. */
    def getSASTokenProvider(abfsConf: AnyRef): Unit

    /** A `String` constant of `AuthConfigurations`, by field name. */
    def authDefault(fieldName: String): String
  }

  // Keyed by the AbfsConfiguration class itself: a ClassValue holds nothing that pins a loader,
  // and `computeValue` throwing caches nothing, so a loader that gains hadoop-azure later
  // (ADD JAR) is probed again on the next call.
  private val cache = new ClassValue[Handles] {
    override protected def computeValue(abfsConfigurationClass: Class[_]): Handles =
      new ReflectiveHandles(abfsConfigurationClass)
  }

  /** The loader Hadoop's `FileSystem.get` would instantiate the ABFS classes with. */
  def loaderFor(hadoopConf: Configuration): ClassLoader =
    Option(hadoopConf.getClassLoader)
      .getOrElse(ClassLoaders.contextOrDefault(getClass.getClassLoader))

  /** The handles for `loader`'s hadoop-azure, or the failure that keeps it from loading. */
  def handles(loader: ClassLoader): Try[Handles] =
    attempt(cache.get(loadClass(loader, ClassNames.ABFS_CONFIGURATION)))

  // `Try` lets a `LinkageError` through, and a hadoop-azure whose own dependencies are missing
  // fails with exactly that (`NoClassDefFoundError`), so it is caught here alongside `NonFatal`.
  private def attempt[A](body: => A): Try[A] = {
    try {
      Success(body)
    } catch {
      case e @ (_: LinkageError | NonFatal(_)) => Failure(e)
    }
  }

  private def loadClass(loader: ClassLoader, name: String): Class[_] =
    ClassLoaders.loadClass(name, loader)

  // Sibling classes come from the loader that defined AbfsConfiguration, the one hadoop-azure
  // itself links against.
  private final class ReflectiveHandles(abfsConfigurationClass: Class[_]) extends Handles {
    private val loader = abfsConfigurationClass.getClassLoader
    private val authTypeClass = loadClass(loader, ClassNames.AUTH_TYPE)
    private val authConfigurationsClass = loadClass(loader, ClassNames.AUTH_CONFIGURATIONS)

    override val accessTokenProviderClass: Class[_] =
      loadClass(loader, ClassNames.ACCESS_TOKEN_PROVIDER)
    override val sasTokenProviderClass: Class[_] =
      loadClass(loader, ClassNames.SAS_TOKEN_PROVIDER)

    private def isLoadable(name: String): Boolean = attempt(loadClass(loader, name)).isSuccess

    override val hasFixedSasTokenProvider: Boolean = isLoadable(
      ClassNames.FIXED_SAS_TOKEN_PROVIDER)
    override val hasClientAssertionProvider: Boolean =
      isLoadable(ClassNames.CLIENT_ASSERTION_PROVIDER)

    // (Configuration, account, container, AbfsServiceType) exists from 3.4.2; the service type
    // enum arrived with it, so a missing enum means the two-argument constructor.
    private val serviceTypeClass: Option[Class[_]] =
      attempt(loadClass(loader, ClassNames.ABFS_SERVICE_TYPE)).toOption
    private val containerConstructor: Option[Constructor[_]] = serviceTypeClass.flatMap { st =>
      val ctor: Try[Constructor[_]] = attempt(
        abfsConfigurationClass
          .getConstructor(classOf[Configuration], classOf[String], classOf[String], st))
      ctor.toOption
    }
    private val accountConstructor: Constructor[_] =
      abfsConfigurationClass.getConstructor(classOf[Configuration], classOf[String])

    private val getAuthTypeMethod =
      abfsConfigurationClass.getMethod("getAuthType", classOf[String])
    private val getMethod = abfsConfigurationClass.getMethod("get", classOf[String])
    private val getPasswordStringMethod =
      abfsConfigurationClass.getMethod("getPasswordString", classOf[String])
    private val getTokenProviderClassMethod = abfsConfigurationClass.getMethod(
      "getTokenProviderClass",
      authTypeClass,
      classOf[String],
      classOf[Class[_]],
      classOf[Class[_]])
    private val getStorageAccountKeyMethod =
      abfsConfigurationClass.getMethod("getStorageAccountKey")
    private val getTokenProviderMethod = abfsConfigurationClass.getMethod("getTokenProvider")
    private val getSASTokenProviderMethod =
      abfsConfigurationClass.getMethod("getSASTokenProvider")

    override def hasContainerScopedConfiguration: Boolean = containerConstructor.isDefined

    override def newAbfsConfiguration(
        conf: Configuration,
        account: String,
        container: String,
        uri: URI): AnyRef = {
      (containerConstructor, serviceTypeClass) match {
        case (Some(ctor), Some(st)) =>
          val serviceTypeName = if (uri.toString.contains(BLOB_DOMAIN_MARKER)) "BLOB" else "DFS"
          construct(ctor, conf, account, container, enumConstant(st, serviceTypeName))
        case _ =>
          construct(accountConstructor, conf, account)
      }
    }

    override def getAuthType(abfsConf: AnyRef, account: String): Enum[_] =
      invoke(getAuthTypeMethod, abfsConf, account).asInstanceOf[Enum[_]]

    override def get(abfsConf: AnyRef, key: String): Option[String] =
      Option(invoke(getMethod, abfsConf, key)).map(_.asInstanceOf[String])

    override def getPasswordString(abfsConf: AnyRef, key: String): Option[String] =
      Option(invoke(getPasswordStringMethod, abfsConf, key)).map(_.asInstanceOf[String])

    override def getTokenProviderClass(
        abfsConf: AnyRef,
        authType: Enum[_],
        key: String,
        xface: Class[_]): Option[Class[_]] = {
      Option(invoke(getTokenProviderClassMethod, abfsConf, authType, key, null, xface))
        .map(_.asInstanceOf[Class[_]])
    }

    override def getStorageAccountKey(abfsConf: AnyRef): String =
      invoke(getStorageAccountKeyMethod, abfsConf).asInstanceOf[String]

    override def getTokenProvider(abfsConf: AnyRef): Unit = {
      invoke(getTokenProviderMethod, abfsConf)
      ()
    }

    override def getSASTokenProvider(abfsConf: AnyRef): Unit = {
      invoke(getSASTokenProviderMethod, abfsConf)
      ()
    }

    override def authDefault(fieldName: String): String =
      authConfigurationsClass.getField(fieldName).get(null).asInstanceOf[String]

    private def enumConstant(enumClass: Class[_], name: String): AnyRef =
      enumClass.getEnumConstants
        .find(_.asInstanceOf[Enum[_]].name == name)
        .getOrElse(throw new NoSuchFieldException(s"${enumClass.getName}.$name"))
        .asInstanceOf[AnyRef]

    // Reflection wraps whatever the target throws; hand the resolver Hadoop's own exception.
    private def invoke(method: Method, target: AnyRef, args: AnyRef*): AnyRef = {
      try {
        method.invoke(target, args: _*)
      } catch {
        case e: InvocationTargetException => throw unwrapped(e)
      }
    }

    private def construct(ctor: Constructor[_], args: AnyRef*): AnyRef = {
      try {
        ctor.newInstance(args: _*).asInstanceOf[AnyRef]
      } catch {
        case e: InvocationTargetException => throw unwrapped(e)
      }
    }

    private def unwrapped(e: InvocationTargetException): Throwable =
      Option(e.getCause).getOrElse(e)
  }
}

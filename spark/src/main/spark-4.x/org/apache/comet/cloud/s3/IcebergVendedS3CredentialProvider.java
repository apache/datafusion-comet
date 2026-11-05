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

package org.apache.comet.cloud.s3;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.comet.util.ClassLoaders;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Resolves S3 credentials for native Iceberg scans the way Iceberg's own {@code S3FileIO} does, so
 * credentials vended by a REST catalog are refreshed instead of expiring mid-job.
 *
 * <p>Without a provider, the native FileIO signs every request with the {@code s3.*} credentials
 * the catalog vended when the driver loaded the table. Vended credentials are short-lived, so a
 * scan still running after they expire fails with {@code ExpiredToken}, while Spark's own read of
 * the same table refreshes them through {@code client.refresh-credentials-endpoint}.
 *
 * <p>This provider hands the FileIO property bag to Iceberg's {@code AwsClientProperties}, which
 * picks the same credentials provider {@code S3FileIO} would: a {@code VendedCredentialsProvider}
 * when the catalog returned a refresh endpoint, the static vended keys otherwise, and the AWS
 * default chain when neither is set. Iceberg is loaded by reflection from the context class loader,
 * as elsewhere in Comet, so Comet keeps no compile-time Iceberg dependency.
 *
 * <p>Comet selects this provider automatically when the FileIO properties carry a refresh endpoint
 * and no provider is configured (see {@code CometIcebergNativeScan.withVendedCredentialProvider}).
 * To name it explicitly: {@code spark.sql.catalog.<cat>.s3.comet.credential.provider.class =
 * org.apache.comet.cloud.s3.IcebergVendedS3CredentialProvider}.
 */
public final class IcebergVendedS3CredentialProvider implements CometS3CredentialProvider {

  static final String AWS_CLIENT_PROPERTIES_CLASS = "org.apache.iceberg.aws.AwsClientProperties";

  // S3FileIOProperties.ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN and
  // SESSION_TOKEN_EXPIRES_AT_MS.
  static final String ACCESS_KEY_ID = "s3.access-key-id";
  static final String SECRET_ACCESS_KEY = "s3.secret-access-key";
  static final String SESSION_TOKEN = "s3.session-token";
  static final String SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

  private static final String ANONYMOUS_HINT =
      "Iceberg resolved this credentials provider from the catalog's FileIO properties (for"
          + " example client.credentials-provider). To read the table without Comet's credential"
          + " provider, set spark.sql.catalog.<catalog>.s3.comet.credential.provider.class to an"
          + " empty value.";

  private volatile AwsCredentialsProvider delegate;
  private volatile String vendedAccessKeyId;
  private volatile long vendedExpirationEpochMillis;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    ClassLoader loader = ClassLoaders.contextOrDefault(getClass().getClassLoader());
    Class<?> clientPropertiesClass;
    try {
      clientPropertiesClass = Class.forName(AWS_CLIENT_PROPERTIES_CLASS, true, loader);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new IllegalStateException(
          "IcebergVendedS3CredentialProvider needs iceberg-aws ("
              + AWS_CLIENT_PROPERTIES_CLASS
              + ") on the application class path",
          e);
    }
    Object provider;
    try {
      Object clientProperties =
          clientPropertiesClass.getConstructor(Map.class).newInstance(catalogProperties);
      Method credentialsProvider =
          clientPropertiesClass.getMethod(
              "credentialsProvider", String.class, String.class, String.class);
      provider =
          credentialsProvider.invoke(
              clientProperties,
              catalogProperties.get(ACCESS_KEY_ID),
              catalogProperties.get(SECRET_ACCESS_KEY),
              catalogProperties.get(SESSION_TOKEN));
    } catch (InvocationTargetException e) {
      throw new IllegalStateException(
          "Iceberg AwsClientProperties failed to build a credentials provider", e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "This iceberg-aws version has no AwsClientProperties.credentialsProvider(String, String,"
              + " String)",
          e);
    }
    this.delegate = asSdkProvider(provider);
    this.vendedAccessKeyId = catalogProperties.get(ACCESS_KEY_ID);
    this.vendedExpirationEpochMillis = parseEpochMillis(catalogProperties);
  }

  /**
   * Casts Iceberg's provider to the AWS SDK interface Comet links against. The two only match when
   * the SDK is loaded once, by a loader both Iceberg and Comet see.
   */
  private static AwsCredentialsProvider asSdkProvider(Object provider) {
    try {
      return (AwsCredentialsProvider) provider;
    } catch (NoClassDefFoundError e) {
      throw new IllegalStateException(
          "IcebergVendedS3CredentialProvider needs the AWS SDK v2 on Comet's class path, next to"
              + " the Comet jar",
          e);
    } catch (ClassCastException e) {
      throw new IllegalStateException(
          "Iceberg returned a credentials provider from a different copy of the AWS SDK than the"
              + " one Comet loaded. This happens when the SDK is on both the application and the"
              + " Spark class paths with spark.{driver,executor}.userClassPathFirst=true; keep one"
              + " copy of the SDK.",
          e);
    }
  }

  private static long parseEpochMillis(Map<String, String> catalogProperties) {
    String value = catalogProperties.get(SESSION_TOKEN_EXPIRES_AT_MS);
    if (value == null || value.trim().isEmpty()) {
      return 0L;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) {
    AwsCredentialsProvider p = delegate;
    if (p == null) {
      throw new IllegalStateException(
          "IcebergVendedS3CredentialProvider used before initialize(Map) was called; "
              + "Comet should always invoke initialize before getCredentialsForPath");
    }
    AwsCredentials c = p.resolveCredentials();
    CometS3Credentials credentials =
        SdkCredentialExtraction.toCometCredentials(context.getBucket(), c, ANONYMOUS_HINT);
    // Static vended keys carry no expiry in the SDK object, so report the one the catalog vended
    // with them. Comet then keeps reusing the credential until shortly before it expires.
    if (credentials.getExpirationEpochMillis() == 0L
        && vendedExpirationEpochMillis > 0L
        && c.accessKeyId().equals(vendedAccessKeyId)) {
      return new CometS3Credentials(
          credentials.getAccessKeyId(),
          credentials.getSecretAccessKey(),
          credentials.getSessionToken(),
          vendedExpirationEpochMillis);
    }
    return credentials;
  }

  @Override
  public void close() throws Exception {
    AwsCredentialsProvider p = delegate;
    if (p instanceof AutoCloseable) {
      ((AutoCloseable) p).close();
    }
  }
}

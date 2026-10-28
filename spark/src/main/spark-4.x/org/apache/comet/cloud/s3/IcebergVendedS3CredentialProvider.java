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
import java.time.Instant;
import java.util.Map;

import org.apache.comet.util.ClassLoaders;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Resolves S3 credentials for native Iceberg reads the way Iceberg's own {@code S3FileIO} does, so
 * credentials vended by a REST catalog are refreshed instead of expiring mid-job.
 *
 * <p>Without a provider, the native scan signs every request with the {@code s3.*} credentials the
 * catalog vended when the driver loaded the table. Unity Catalog vends credentials that last about
 * an hour, so a scan still running after that fails with {@code ExpiredToken}, while vanilla Spark
 * on the same table refreshes them through {@code client.refresh-credentials-endpoint}.
 *
 * <p>This provider hands the FileIO property bag to Iceberg's {@code AwsClientProperties}, which
 * picks the same credentials provider {@code S3FileIO} would: a {@code VendedCredentialsProvider}
 * when the catalog returned a refresh endpoint, the static vended keys otherwise, and the AWS
 * default chain when neither is set. Iceberg is loaded by reflection, as elsewhere in Comet, so
 * Comet keeps no compile-time Iceberg dependency and the class resolves from whichever loader
 * carries Iceberg at runtime.
 *
 * <p>Activation: {@code spark.sql.catalog.<cat>.s3.comet.credential.provider.class =
 * org.apache.comet.cloud.s3.IcebergVendedS3CredentialProvider}.
 */
public final class IcebergVendedS3CredentialProvider implements CometS3CredentialProvider {

  static final String AWS_CLIENT_PROPERTIES_CLASS = "org.apache.iceberg.aws.AwsClientProperties";

  // S3FileIOProperties.ACCESS_KEY_ID, SECRET_ACCESS_KEY and SESSION_TOKEN.
  static final String ACCESS_KEY_ID = "s3.access-key-id";
  static final String SECRET_ACCESS_KEY = "s3.secret-access-key";
  static final String SESSION_TOKEN = "s3.session-token";

  private volatile AwsCredentialsProvider delegate;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    ClassLoader loader = ClassLoaders.contextOrDefault(getClass().getClassLoader());
    try {
      Class<?> clientPropertiesClass = Class.forName(AWS_CLIENT_PROPERTIES_CLASS, true, loader);
      Object clientProperties =
          clientPropertiesClass.getConstructor(Map.class).newInstance(catalogProperties);
      Method credentialsProvider =
          clientPropertiesClass.getMethod(
              "credentialsProvider", String.class, String.class, String.class);
      this.delegate =
          (AwsCredentialsProvider)
              credentialsProvider.invoke(
                  clientProperties,
                  catalogProperties.get(ACCESS_KEY_ID),
                  catalogProperties.get(SECRET_ACCESS_KEY),
                  catalogProperties.get(SESSION_TOKEN));
    } catch (InvocationTargetException e) {
      throw new IllegalStateException(
          "Iceberg AwsClientProperties failed to build a credentials provider", e.getCause());
    } catch (ReflectiveOperationException | LinkageError e) {
      throw new IllegalStateException(
          "IcebergVendedS3CredentialProvider needs iceberg-aws ("
              + AWS_CLIENT_PROPERTIES_CLASS
              + ") on the classpath",
          e);
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
    String sessionToken = null;
    long expirationEpochMillis = 0L;
    if (c instanceof AwsSessionCredentials) {
      AwsSessionCredentials session = (AwsSessionCredentials) c;
      sessionToken = session.sessionToken();
      // Report the expiry so Comet reuses the credential until shortly before it and asks again.
      // VendedCredentialsProvider's own cache decides when that ask calls the refresh endpoint.
      expirationEpochMillis = session.expirationTime().map(Instant::toEpochMilli).orElse(0L);
    }
    return new CometS3Credentials(
        c.accessKeyId(), c.secretAccessKey(), sessionToken, expirationEpochMillis);
  }

  @Override
  public void close() throws Exception {
    AwsCredentialsProvider p = delegate;
    if (p instanceof AutoCloseable) {
      ((AutoCloseable) p).close();
    }
  }
}

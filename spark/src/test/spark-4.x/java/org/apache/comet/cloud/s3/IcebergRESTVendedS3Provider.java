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

import java.util.Map;

import org.apache.iceberg.aws.s3.VendedCredentialsProvider;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Example implementation of {@link CometS3CredentialProvider} for Iceberg REST catalogs that vend
 * AWS credentials. Wraps Iceberg's {@code VendedCredentialsProvider} so caching and
 * refresh-near-expiry come from its {@code CachedSupplier}; Comet adds only the JNI shape and the
 * one-shot {@code initialize} call.
 *
 * <p>Test scope only, to keep iceberg-aws and AWS SDK v2 off Comet's runtime classpath. Production
 * users should copy this into their own jar.
 *
 * <p>Spark 4.x build only (Iceberg 1.9.0+ is required for the in-properties caching short-circuit
 * in {@code VendedCredentialsProvider}; earlier pins issue an HTTP GET against {@code
 * credentials.uri} on every refresh).
 *
 * <p>Activation: set {@code spark.sql.catalog.<cat>.s3.comet.credential.provider.class =
 * org.apache.comet.cloud.s3.IcebergRESTVendedS3Provider}. Comet calls {@link #initialize} once per
 * catalog with the unfiltered FileIO property bag, which carries {@code credentials.uri} and
 * {@code uri} as required by {@code VendedCredentialsProvider.create}.
 */
public final class IcebergRESTVendedS3Provider implements CometS3CredentialProvider {

  private volatile VendedCredentialsProvider provider;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.provider = VendedCredentialsProvider.create(catalogProperties);
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) {
    VendedCredentialsProvider p = provider;
    if (p == null) {
      throw new IllegalStateException(
          "IcebergRESTVendedS3Provider used before initialize(Map) was called; "
              + "Comet should always invoke initialize before getCredentialsForPath");
    }
    AwsCredentials c = p.resolveCredentials();
    String sessionToken =
        (c instanceof AwsSessionCredentials) ? ((AwsSessionCredentials) c).sessionToken() : null;
    // Expiration is owned by VendedCredentialsProvider's CachedSupplier; we publish 0 so the
    // native bridge applies its conservative floor to `opendal`'s cache while the inner
    // CachedSupplier handles refresh on its own schedule.
    return new CometS3Credentials(c.accessKeyId(), c.secretAccessKey(), sessionToken, 0L);
  }
}

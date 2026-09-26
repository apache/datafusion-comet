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

import java.util.List;

import org.apache.comet.annotation.Public;

/**
 * Opt-in extension of {@link CometS3CredentialProvider} for buckets whose credentials differ by
 * location, for example one policy for {@code warehouse/sales} and another for {@code
 * warehouse/finance}. Implementing it tells Comet to request a credential per location rather than
 * one per bucket.
 *
 * <p>Each request is served with the credential of the longest location that covers its path. A
 * location covers a path when the path is the location itself or lies below it, compared one path
 * segment at a time: {@code warehouse/sales} covers {@code warehouse/sales/part-0.parquet} but not
 * {@code warehouse/sales_eu/part-0.parquet}. When locations nest, the longer one wins. The bucket
 * root is an implicit location that covers every path no returned location covers.
 *
 * <p>Comet requests a location's credential by calling {@link
 * #getCredentialsForPath(CometS3CredentialContext)} with the location as the context's path, as
 * returned but with a leading slash ({@code /} for the bucket root). The credential must authorize
 * every path for which that location is the longest covering one.
 *
 * <p>Locations apply to Comet's native Parquet reads. Iceberg reads do not use them: there Comet
 * calls {@link #getCredentialsForPath(CometS3CredentialContext)} as it does for any provider.
 *
 * <p>Providers that implement only {@link CometS3CredentialProvider} are unaffected and keep one
 * credential per bucket.
 */
@Public
public interface CometS3LocationScopedCredentialProvider extends CometS3CredentialProvider {

  /**
   * Returns every location in {@code bucket} that has its own credential policy.
   *
   * <p>Locations are written like {@link CometS3CredentialContext#getPath()}: the path within the
   * bucket, percent-encoded as in an {@code s3://} URI, without the scheme or bucket name. A
   * literal {@code %} must be written as {@code %25}; other characters may be left unencoded. A
   * leading or trailing {@code /} is optional. Comet percent-decodes locations and request paths
   * before comparing them, and when several locations decode to the same path it keeps the first.
   *
   * <p>A location is invalid if, once decoded, it is not valid UTF-8 or has a segment that is
   * empty, {@code .}, {@code ..}, or contains a control character, so a URI such as {@code
   * s3://bucket/a} is invalid too. An invalid or {@code null} location fails the read, as does a
   * {@code null} list.
   *
   * <p>Comet treats the result as a snapshot and may keep it for the life of an executor. It asks
   * again after a request fails with 403, so a location added or removed later may not take effect
   * until then. It may call this on the driver or on executors, and from several threads at once.
   *
   * @param bucket the S3 bucket name, without scheme or path
   * @return the bucket's locations, or an empty list if every path uses the bucket-root credential
   * @throws Exception if the locations cannot be determined. Comet then fails the read rather than
   *     fall back to a broader credential.
   */
  List<String> getPolicyLocations(String bucket) throws Exception;
}

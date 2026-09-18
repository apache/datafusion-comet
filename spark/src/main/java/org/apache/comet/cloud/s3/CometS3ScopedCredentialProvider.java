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

import java.util.Collections;
import java.util.List;

import org.apache.comet.annotation.Public;

/**
 * Opt-in extension of {@link CometS3CredentialProvider} that lets a vendor advertise the scope of
 * the STS session it is about to vend, so Comet can cache one {@code ObjectStore} per distinct
 * scope on a bucket instead of the single per-bucket store used for base providers.
 *
 * <p>Comet native does an {@code instanceof} check at bridge construction: implementors get a
 * scope-aware cache entry keyed on the returned prefix list; providers that only implement the base
 * {@link CometS3CredentialProvider} keep the single-entry-per-bucket cache. Existing vendor code
 * compiled against earlier versions is unaffected.
 *
 * <p>The prefix return is <strong>advisory</strong>. S3 remains authoritative: Comet wraps every
 * cached store in a 403-retry safety net that invalidates the entry and re-fires the SPI with the
 * failing path in context. Over-reporting is self-healing at the cost of one 403 per newly
 * discovered scope boundary; under-reporting only costs extra SPI churn. Vendors that are uncertain
 * should return {@link Collections#emptyList()} — which Comet treats as "no scope hint" and falls
 * back to single-entry-per-bucket behavior.
 */
@Public
public interface CometS3ScopedCredentialProvider extends CometS3CredentialProvider {

  /**
   * Returns the narrowest known-safe S3-key prefixes that the STS session this provider is about to
   * vend for {@code context} will authorize on the bucket. Comet uses these as cache-scope hints
   * only.
   *
   * <p>Prefixes are relative to the bucket root (no scheme, no leading slash). A path {@code P} is
   * considered covered by a prefix {@code Q} iff, after both are canonicalized to bucket-relative
   * form, {@code P == Q} or {@code P.startsWith(Q + "/")}. An empty list disables the scope-aware
   * cache for this request (single-entry-per-bucket behavior).
   *
   * <p>Vendors should err on the side of narrower reporting. If the underlying policy source cannot
   * answer cheaply, return {@link Collections#emptyList()} — the correctness ground is Comet's
   * 403-retry wrapper, not this hint.
   *
   * @param context the same context passed to {@link
   *     #getCredentialsForPath(CometS3CredentialContext)}
   * @return bucket-relative prefixes, or {@link Collections#emptyList()} for "no hint"; must be
   *     non-null
   */
  List<String> getPolicyLocationsFor(CometS3CredentialContext context) throws Exception;
}

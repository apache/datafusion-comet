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

package org.apache.comet.cloud;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// spotless:off
/*
 * Architecture Overview:
 *
 *                 JVM Side                              |                  Native Side
 *   ┌──────────────────────────────────────────┐       |     ┌──────────────────────────────────────────┐
 *   │     CometCloudCredentialDispatcher       │       |     │        S3 Object Reading                 │
 *   │                                          │       |     │                                          │
 *   │  ┌────────────────────────────────────┐  │       |     │  ┌────────────────────────────────────┐  │
 *   │  │  ServiceLoader discovery:          │  │       |     │  │  DataFusion       iceberg-rust     │  │
 *   │  │  META-INF/services/                │  │       |     │  │  object_store        opendal       │  │
 *   │  │   o.a.c.cloud.CometCloudCred...    │  │       |     │  └────────────────────────────────────┘  │
 *   │  └────────────────────────────────────┘  │       |     │             │              │             │
 *   │              │                           │       |     │             ▼              ▼             │
 *   │              ▼                           │       |     │  ┌────────────────────────────────────┐  │
 *   │  ┌────────────────────────────────────┐  │       |     │  │   CometCredentialBridge (Rust)     │  │
 *   │  │  CometCloudCredentialProvider      │  │       |     │  │    impl object_store::             │  │
 *   │  │   (single instance, cached)        │  │       |     │  │         CredentialProvider         │  │
 *   │  └────────────────────────────────────┘  │       |     │  │    impl reqsign_core::             │  │
 *   │              │                           │       |     │  │         ProvideCredential          │  │
 *   │              ▼                           │       |     │  └────────────────────────────────────┘  │
 *   │  ┌────────────────────────────────────┐  │       |     │               │                          │
 *   │  │  .getCredentialsForPath(...)       │◄─┼───────┼─────┼──╗            ▼                          │
 *   │  └────────────────────────────────────┘  │       |     │  ╔════════════════════════════════════╗  │
 *   │              │                           │       |     │  ║          JNI CALL:                 ║  │
 *   │              ▼                           │       |     │  ║    getCredentialsForPath(          ║  │
 *   │  ┌────────────────────────────────────┐  │       |     │  ║        bucket, path)               ║  │
 *   │  │  return CometCredentials POJO      │──┼───────┼─────┼─►║                                    ║  │
 *   │  │  (access key, secret, token,       │  │       |     │  ╚════════════════════════════════════╝  │
 *   │  │   region, expiration)              │  │       |     │              │                           │
 *   │  └────────────────────────────────────┘  │       |     │              ▼                           │
 *   │                                          │       |     │  ┌────────────────────────────────────┐  │
 *   │                                          │       |     │  │   AwsCredential                    │  │
 *   │                                          │       |     │  │   used to sign S3 requests         │  │
 *   │                                          │       |     │  └────────────────────────────────────┘  │
 *   └──────────────────────────────────────────┘       |     └──────────────────────────────────────────┘
 *                                                      |
 *                                              JNI Boundary
 *
 * Setup Phase (one-time per executor):
 * 1. Vendor JAR ships an impl of CometCloudCredentialProvider via META-INF/services.
 * 2. CometCloudCredentialDispatcher resolves it via ServiceLoader on first class-load.
 * 3. Native side caches dispatcher class + static method ID in OnceCell.
 *
 * Runtime Phase (per S3 request):
 * 4. object_store / opendal calls its async credential trait on CometCredentialBridge.
 * 5. Bridge enters JNI, invokes dispatcher.getCredentialsForPath(bucket, path).
 * 6. Provider returns a CometCredentials POJO; vendor may call its own STS / authorization service.
 * 7. Rust reads fields via JNI accessors, returns AwsCredential for request signing.
 */
// spotless:on

/**
 * Static entry point invoked from Comet's native code (via JNI) to fetch AWS credentials for an S3
 * request.
 *
 * <p>Resolution rules at first class-load:
 *
 * <ul>
 *   <li>Zero impls registered via {@code ServiceLoader} → {@link #isProviderRegistered()} returns
 *       false; native callers fall through to the existing AWS credential chain.
 *   <li>Exactly one impl registered → cached and used for every credential request.
 *   <li>Multiple impls registered → throws {@link IllegalStateException} at class-load, failing the
 *       executor loudly. Operators should remove all but one bridge jar.
 * </ul>
 *
 * <p>Discovery is via classpath only; there is no Comet-specific config knob for selecting a
 * provider. This keeps the credentials Comet uses identical to whatever the same JVM would use if a
 * query fell back to Spark execution mid-flight.
 */
public final class CometCloudCredentialDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CometCloudCredentialDispatcher.class);

  /*
   * Process-lifetime singleton, justified per the contributor guide's "Global singletons"
   * section.
   *
   * Why static is the right lifetime: ServiceLoader discovers the impl from the executor
   * classpath, which is fixed once Spark has launched the JVM. The same instance must serve
   * every credential request from native code so that a query falling back from Comet to
   * Spark mid-execution sees identical credentials.
   *
   * Bounded: a single reference, not a cache.
   *
   * Credential refresh: this dispatcher does NOT cache credentials. Each call to
   * getCredentialsForPath delegates straight to the provider, which is responsible for any
   * STS / token refresh logic. Stale-credential failure modes therefore live in the provider
   * impl, not here.
   */
  private static final CometCloudCredentialProvider PROVIDER = resolve();

  private CometCloudCredentialDispatcher() {}

  /** Returns true if a provider was discovered on the classpath. */
  public static boolean isProviderRegistered() {
    return PROVIDER != null;
  }

  /**
   * Invoked by native code via JNI. Delegates to the registered provider.
   *
   * @throws IllegalStateException if no provider is registered (callers should check {@link
   *     #isProviderRegistered()} first)
   */
  public static CometCredentials getCredentialsForPath(String bucket, String path)
      throws Exception {
    if (PROVIDER == null) {
      throw new IllegalStateException(
          "No CometCloudCredentialProvider registered; check META-INF/services on the classpath");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fetching credentials for bucket={} path={}", bucket, path);
    }
    return PROVIDER.getCredentialsForPath(bucket, path);
  }

  private static CometCloudCredentialProvider resolve() {
    List<CometCloudCredentialProvider> impls = new ArrayList<>();
    Iterator<CometCloudCredentialProvider> it =
        ServiceLoader.load(CometCloudCredentialProvider.class).iterator();
    while (it.hasNext()) {
      impls.add(it.next());
    }
    if (impls.isEmpty()) {
      LOG.info(
          "No CometCloudCredentialProvider registered; native S3 readers will use the default "
              + "AWS credential chain");
      return null;
    }
    if (impls.size() > 1) {
      List<String> names = new ArrayList<>(impls.size());
      for (CometCloudCredentialProvider impl : impls) {
        names.add(impl.getClass().getName());
      }
      LOG.error("Multiple CometCloudCredentialProvider impls on classpath: {}", names);
      throw new IllegalStateException(
          "Multiple CometCloudCredentialProvider impls on classpath: " + names);
    }
    CometCloudCredentialProvider provider = impls.get(0);
    LOG.info("Registered CometCloudCredentialProvider: {}", provider.getClass().getName());
    return provider;
  }
}

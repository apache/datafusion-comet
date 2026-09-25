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

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;

import org.apache.comet.util.ClassLoaders;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Wraps a raw AWS SDK v2 {@link AwsCredentialsProvider} named via {@code
 * fs.s3a.comet.credential.adapter.class}, for a provider not registered through S3A. This is the
 * spark-4.x (SDK v2) body. Prefer {@link HadoopS3ACredentialProviderAdapter} unless the provider is
 * a plain SDK class not wired through Hadoop.
 *
 * <pre>
 * spark.hadoop.fs.s3a.comet.credential.provider.class=org.apache.comet.cloud.s3.AwsSdkCredentialProviderAdapter
 * spark.hadoop.fs.s3a.comet.credential.adapter.class=&lt;FQCN of an AwsCredentialsProvider&gt;
 * </pre>
 */
public class AwsSdkCredentialProviderAdapter implements CometS3CredentialProvider {

  static final String DELEGATE_CLASS_PROPERTY = "comet.credential.adapter.class";

  private Map<String, String> properties;
  // Captured on the thread that runs initialize() (the dispatcher calls it during planning, which
  // has Spark's user-jar loader). getCredentialsForPath runs on native worker threads whose context
  // ClassLoader is null, so the delegate must be loaded with this captured loader, not the TCCL.
  private volatile ClassLoader classLoader;
  private final ConcurrentHashMap<String, AwsCredentialsProvider> delegates =
      new ConcurrentHashMap<>();

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.properties = catalogProperties;
    this.classLoader = ClassLoaders.contextOrDefault(getClass().getClassLoader());
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context)
      throws Exception {
    AwsCredentialsProvider provider = ensureDelegate(context.getBucket());
    return SdkCredentialExtraction.toCometCredentials(provider.resolveCredentials());
  }

  private AwsCredentialsProvider ensureDelegate(String bucket) throws Exception {
    AwsCredentialsProvider existing = delegates.get(bucket);
    if (existing != null) {
      return existing;
    }
    synchronized (this) {
      AwsCredentialsProvider delegate = delegates.get(bucket);
      if (delegate == null) {
        delegate = instantiate(bucket);
        delegates.put(bucket, delegate);
      }
      return delegate;
    }
  }

  private AwsCredentialsProvider instantiate(String bucket) throws Exception {
    String className = AdapterSupport.lookup(properties, bucket, DELEGATE_CLASS_PROPERTY);
    if (className == null) {
      throw new IllegalStateException(
          "AwsSdkCredentialProviderAdapter requires fs.s3a."
              + DELEGATE_CLASS_PROPERTY
              + " (or the per-bucket variant) to name an AwsCredentialsProvider");
    }
    Class<?> clazz = ClassLoaders.loadClass(className, classLoader);
    if (!AwsCredentialsProvider.class.isAssignableFrom(clazz)) {
      throw new IllegalStateException(className + " does not implement AwsCredentialsProvider");
    }
    Configuration conf =
        S3AUtils.propagateBucketOptions(AdapterSupport.toConfiguration(properties), bucket);
    AdapterSupport.patchSecurityCredentialProviders(conf);
    URI uri = new URI("s3a://" + bucket + "/");
    return (AwsCredentialsProvider)
        AdapterSupport.instantiateDelegate(AwsCredentialsProvider.class, clazz, uri, conf);
  }

  @Override
  public void close() throws Exception {
    for (AwsCredentialsProvider delegate : delegates.values()) {
      if (delegate instanceof AutoCloseable) {
        ((AutoCloseable) delegate).close();
      }
    }
  }
}

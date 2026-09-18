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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.comet.annotation.Public;
import org.apache.comet.util.ClassLoaders;

/**
 * Wraps a raw AWS SDK v2 {@link AwsCredentialsProvider} named via
 * {@code fs.s3a.comet.credential.adapter.class}, for a provider not registered through S3A. This is
 * the spark-4.x (SDK v2) body. Prefer {@link HadoopS3ACredentialProviderAdapter} unless the
 * provider is a plain SDK class not wired through Hadoop.
 *
 * <pre>
 * spark.hadoop.fs.s3a.comet.credential.provider.class=org.apache.comet.cloud.s3.AwsSdkCredentialProviderAdapter
 * spark.hadoop.fs.s3a.comet.credential.adapter.class=&lt;FQCN of an AwsCredentialsProvider&gt;
 * </pre>
 */
@Public
public class AwsSdkCredentialProviderAdapter implements CometS3CredentialProvider {

  static final String DELEGATE_CLASS_PROPERTY = "comet.credential.adapter.class";

  private volatile Map<String, String> properties = new HashMap<>();
  private volatile String resolvedBucket;
  private volatile AwsCredentialsProvider delegate;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.properties = catalogProperties;
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context)
      throws Exception {
    AwsCredentialsProvider provider = ensureDelegate(context.getBucket());
    return SdkCredentialExtraction.toCometCredentials(provider.resolveCredentials());
  }

  private AwsCredentialsProvider ensureDelegate(String bucket) throws Exception {
    AwsCredentialsProvider local = delegate;
    if (local != null && bucket.equals(resolvedBucket)) {
      return local;
    }
    synchronized (this) {
      if (delegate == null || !bucket.equals(resolvedBucket)) {
        delegate = instantiate(bucket);
        resolvedBucket = bucket;
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
    Class<?> clazz = ClassLoaders.loadClass(className);
    if (!AwsCredentialsProvider.class.isAssignableFrom(clazz)) {
      throw new IllegalStateException(
          className
              + " does not implement software.amazon.awssdk.auth.credentials.AwsCredentialsProvider");
    }
    // SDK v2 instantiation conventions, in order: static create(), static builder().build(),
    // public no-arg constructor.
    Method create = staticMethod(clazz, "create");
    if (create != null) {
      return (AwsCredentialsProvider) create.invoke(null);
    }
    Method builder = staticMethod(clazz, "builder");
    if (builder != null) {
      Object b = builder.invoke(null);
      Method build = b.getClass().getMethod("build");
      return (AwsCredentialsProvider) build.invoke(b);
    }
    return (AwsCredentialsProvider) clazz.getDeclaredConstructor().newInstance();
  }

  private static Method staticMethod(Class<?> clazz, String name) {
    try {
      Method m = clazz.getMethod(name);
      return Modifier.isStatic(m.getModifiers()) ? m : null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  @Override
  public void close() throws Exception {
    AwsCredentialsProvider local = delegate;
    if (local instanceof AutoCloseable) {
      ((AutoCloseable) local).close();
    }
  }
}

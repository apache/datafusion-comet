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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;

import org.apache.comet.annotation.Public;
import org.apache.comet.util.ClassLoaders;

/**
 * Wraps a raw AWS SDK v1 {@link AWSCredentialsProvider} named via
 * {@code fs.s3a.comet.credential.adapter.class}, for a provider not registered through S3A. This is
 * the spark-3.x (SDK v1) body. Prefer {@link HadoopS3ACredentialProviderAdapter} unless the
 * provider is a plain SDK class not wired through Hadoop.
 *
 * <pre>
 * spark.hadoop.fs.s3a.comet.credential.provider.class=org.apache.comet.cloud.s3.AwsSdkCredentialProviderAdapter
 * spark.hadoop.fs.s3a.comet.credential.adapter.class=&lt;FQCN of an AWSCredentialsProvider&gt;
 * </pre>
 */
@Public
public class AwsSdkCredentialProviderAdapter implements CometS3CredentialProvider {

  static final String DELEGATE_CLASS_PROPERTY = "comet.credential.adapter.class";

  private volatile Map<String, String> properties = new HashMap<>();
  private volatile String resolvedBucket;
  private volatile AWSCredentialsProvider delegate;

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    this.properties = catalogProperties;
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context)
      throws Exception {
    AWSCredentialsProvider provider = ensureDelegate(context.getBucket());
    return SdkCredentialExtraction.toCometCredentials(provider.getCredentials());
  }

  private AWSCredentialsProvider ensureDelegate(String bucket) throws Exception {
    AWSCredentialsProvider local = delegate;
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

  private AWSCredentialsProvider instantiate(String bucket) throws Exception {
    String className = AdapterSupport.lookup(properties, bucket, DELEGATE_CLASS_PROPERTY);
    if (className == null) {
      throw new IllegalStateException(
          "AwsSdkCredentialProviderAdapter requires fs.s3a."
              + DELEGATE_CLASS_PROPERTY
              + " (or the per-bucket variant) to name an AWSCredentialsProvider");
    }
    Class<?> clazz = ClassLoaders.loadClass(className);
    if (!AWSCredentialsProvider.class.isAssignableFrom(clazz)) {
      throw new IllegalStateException(
          className + " does not implement com.amazonaws.auth.AWSCredentialsProvider");
    }
    Configuration conf =
        S3AUtils.propagateBucketOptions(AdapterSupport.toConfiguration(properties), bucket);
    URI uri = new URI("s3a://" + bucket + "/");
    // Hadoop-style v1 provider instantiation conventions, in order: (URI, Configuration),
    // (Configuration), static getInstance(), public no-arg constructor.
    Constructor<?> uriConf = constructor(clazz, URI.class, Configuration.class);
    if (uriConf != null) {
      return (AWSCredentialsProvider) uriConf.newInstance(uri, conf);
    }
    Constructor<?> confOnly = constructor(clazz, Configuration.class);
    if (confOnly != null) {
      return (AWSCredentialsProvider) confOnly.newInstance(conf);
    }
    Method getInstance = staticMethod(clazz, "getInstance");
    if (getInstance != null) {
      return (AWSCredentialsProvider) getInstance.invoke(null);
    }
    return (AWSCredentialsProvider) clazz.getDeclaredConstructor().newInstance();
  }

  private static Constructor<?> constructor(Class<?> clazz, Class<?>... params) {
    try {
      return clazz.getConstructor(params);
    } catch (NoSuchMethodException e) {
      return null;
    }
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
    AWSCredentialsProvider local = delegate;
    if (local instanceof AutoCloseable) {
      ((AutoCloseable) local).close();
    }
  }
}
